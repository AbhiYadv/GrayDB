//! bench-cdc: Research Target R1 harness (docs/RESEARCH-R1.md).
//! Measures GrayDB's analytical query latency in a QUIET phase vs under a continuous
//! 90% INSERT / 8% UPDATE / 2% DELETE PostgreSQL workload, plus source->visible
//! freshness and exact-at-LSN correctness probes. This fills the GrayDB column of the
//! R1 table; the ClickHouse column requires the Linux stage (no native Windows build).
//!
//! Scale knobs (local defaults are laptop-sized; the 1B-row run belongs on Linux):
//!   GRAYDB_BENCH_SEED   seed rows in app.orders   (default 1_000_000)
//!   GRAYDB_BENCH_TPS    CDC rows/second           (default 300)
//!   GRAYDB_BENCH_SECS   seconds per phase         (default 45)
//! Run: `just bench-r1` (release build; dev-build numbers are meaningless).

use anyhow::{Context, Result};
use graydb_ingest::repl::{format_lsn, parse_lsn};
use graydb_studio::engine::Engine;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const Q1: &str = "SELECT customer_id, sum(CAST(amount AS DOUBLE)) AS revenue, count(*) AS n \
                  FROM app.orders GROUP BY customer_id";
const Q2: &str = "SELECT status, count(*) AS n FROM app.orders \
                  WHERE customer_id % 100 = 17 GROUP BY status";
const SEED_SQL: &str = include_str!("../../../../db/seed.sql");

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[derive(Default)]
struct PhaseStats {
    q1_ms: Vec<f64>,
    q2_ms: Vec<f64>,
    freshness_ms: Vec<f64>,
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn summarize(label: &str, mut v: Vec<f64>) -> (String, f64, f64, f64) {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let (p50, p95, p99) = (pct(&v, 0.50), pct(&v, 0.95), pct(&v, 0.99));
    (
        format!(
            "{label:<22} n={:<5} p50={p50:>8.1}ms p95={p95:>8.1}ms p99={p99:>8.1}ms max={:>8.1}ms",
            v.len(),
            v.last().copied().unwrap_or(f64::NAN)
        ),
        p50,
        p95,
        p99,
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let seed_rows = env_u64("GRAYDB_BENCH_SEED", 1_000_000);
    let tps = env_u64("GRAYDB_BENCH_TPS", 300);
    let phase_secs = env_u64("GRAYDB_BENCH_SECS", 45);
    println!("== GrayDB bench-cdc (R1-local) ==");
    println!("seed={seed_rows} rows, cdc={tps} rows/s (90/8/2 ins/upd/del), {phase_secs}s per phase");
    println!("build: {}", if cfg!(debug_assertions) { "DEBUG (numbers meaningless!)" } else { "release" });

    // ---- Seed the source --------------------------------------------------------
    let cfg = graydb_ingest::config::Config::load()?;
    let admin = cfg.connect().await?;
    let version: String = admin.query_one("SHOW server_version", &[]).await?.get(0);
    println!("source: {}:{} (PostgreSQL {version})", cfg.source.host, cfg.source.port);

    graydb_ingest::attach::drop_slot_if_exists(&admin, &cfg.source.slot).await?;
    graydb_ingest::attach::drop_publication_if_exists(&admin, &cfg.source.publication).await?;
    println!("[1/5] seeding app.orders to {seed_rows} rows ...");
    admin.batch_execute(SEED_SQL).await.context("seed.sql")?;
    let base: i64 = admin.query_one("SELECT count(*) FROM app.orders", &[]).await?.get(0);
    let mut have = base as u64;
    while have < seed_rows {
        let batch = (seed_rows - have).min(100_000) as i64;
        admin
            .execute(
                "INSERT INTO app.orders (customer_id, status, amount)
                 SELECT 1 + (g % 5000), (ARRAY['new','paid','shipped'])[1 + g % 3],
                        ((g % 999900))::numeric / 100
                 FROM generate_series(1, $1::bigint) g",
                &[&batch],
            )
            .await?;
        have += batch as u64;
    }
    admin.batch_execute("ANALYZE app.orders").await?;
    println!("        app.orders = {have} rows");

    // ---- Attach the full pipeline ------------------------------------------------
    println!("[2/5] attach: slot + backfill + pump + incremental apply ...");
    let t0 = Instant::now();
    let engine = Engine::new(cfg.clone());
    engine.attach().await?;
    println!("        attached in {:.1?}", t0.elapsed());

    let stats = Arc::new(Mutex::new((PhaseStats::default(), PhaseStats::default())));
    let heavy = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let cdc_pause = Arc::new(AtomicBool::new(false));

    // ---- Freshness sampler (runs across both phases) ------------------------------
    let fresh_task = {
        let cfg = cfg.clone();
        let engine = Arc::clone(&engine);
        let stats = Arc::clone(&stats);
        let heavy = Arc::clone(&heavy);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let client = match cfg.connect().await {
                Ok(c) => c,
                Err(_) => return,
            };
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let t = Instant::now();
                if client
                    .execute(
                        "INSERT INTO app.orders (customer_id, status, amount) VALUES (1, 'fresh', 0)",
                        &[],
                    )
                    .await
                    .is_err()
                {
                    continue;
                }
                let head: String = match client
                    .query_one("SELECT pg_current_wal_lsn()::text", &[])
                    .await
                {
                    Ok(r) => r.get(0),
                    Err(_) => continue,
                };
                let Ok(lsn) = parse_lsn(&head) else { continue };
                if engine.wait_applied(lsn, Duration::from_secs(15)).await {
                    let ms = t.elapsed().as_secs_f64() * 1000.0;
                    let mut s = stats.lock().await;
                    if heavy.load(Ordering::Relaxed) {
                        s.1.freshness_ms.push(ms);
                    } else {
                        s.0.freshness_ms.push(ms);
                    }
                }
            }
        })
    };

    // ---- Query loop for one phase --------------------------------------------------
    async fn run_phase(
        engine: &Arc<Engine>,
        secs: u64,
        out: &mut PhaseStats,
    ) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(secs);
        let mut flip = false;
        while Instant::now() < deadline {
            let (sql, bucket_is_q1) = if flip { (Q2, false) } else { (Q1, true) };
            flip = !flip;
            let t = Instant::now();
            engine.query(sql, "eventual").await?;
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            if bucket_is_q1 {
                out.q1_ms.push(ms);
            } else {
                out.q2_ms.push(ms);
            }
        }
        Ok(())
    }

    // ---- Phase 1: quiet -------------------------------------------------------------
    println!("[3/5] QUIET phase: {phase_secs}s of Q1/Q2 with no source writes ...");
    {
        let mut quiet = PhaseStats::default();
        run_phase(&engine, phase_secs, &mut quiet).await?;
        stats.lock().await.0.q1_ms = quiet.q1_ms;
        stats.lock().await.0.q2_ms = quiet.q2_ms;
    }

    // ---- CDC driver -------------------------------------------------------------------
    println!("[4/5] HEAVY phase: {phase_secs}s of Q1/Q2 under {tps} rows/s CDC ...");
    let max_id: i64 = admin.query_one("SELECT max(id) FROM app.orders", &[]).await?.get(0);
    let cdc_task = {
        let cfg = cfg.clone();
        let stop = Arc::clone(&stop);
        let pause = Arc::clone(&cdc_pause);
        tokio::spawn(async move {
            let client = match cfg.connect().await {
                Ok(c) => c,
                Err(_) => return (0u64, 0u64, 0u64),
            };
            let per_tick = (tps as f64 / 10.0).max(1.0);
            let ins_n = (per_tick * 0.90).max(1.0) as i64;
            let upd_n = (per_tick * 0.08).max(1.0) as i64;
            let del_n = (per_tick * 0.02).max(1.0) as i64;
            let (mut ins, mut upd, mut del) = (0u64, 0u64, 0u64);
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            while !stop.load(Ordering::Relaxed) {
                tick.tick().await;
                if pause.load(Ordering::Relaxed) {
                    continue;
                }
                if client.execute(
                    "INSERT INTO app.orders (customer_id, status, amount)
                     SELECT 1 + (g % 5000), 'cdc', ((g % 9999))::numeric / 100
                     FROM generate_series(1, $1::bigint) g",
                    &[&ins_n],
                ).await.is_ok() {
                    ins += ins_n as u64;
                }
                if client.execute(
                    "UPDATE app.orders SET status = 'cdc-upd', amount = amount + 1
                     WHERE id IN (SELECT (1 + floor(random() * $2))::bigint
                                  FROM generate_series(1, $1::bigint))",
                    &[&upd_n, &(max_id as f64)],
                ).await.is_ok() {
                    upd += upd_n as u64;
                }
                if client.execute(
                    "DELETE FROM app.orders
                     WHERE id IN (SELECT (1 + floor(random() * $2))::bigint
                                  FROM generate_series(1, $1::bigint))",
                    &[&del_n, &(max_id as f64)],
                ).await.is_ok() {
                    del += del_n as u64;
                }
            }
            (ins, upd, del)
        })
    };
    heavy.store(true, Ordering::Relaxed);

    // Correctness probes fire inside the heavy phase from a side task.
    let probe_results = Arc::new(Mutex::new(Vec::<(bool, String)>::new()));
    let probe_task = {
        let cfg = cfg.clone();
        let engine = Arc::clone(&engine);
        let pause = Arc::clone(&cdc_pause);
        let results = Arc::clone(&probe_results);
        let secs = phase_secs;
        tokio::spawn(async move {
            let client = match cfg.connect().await {
                Ok(c) => c,
                Err(_) => return,
            };
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_secs(secs / 4)).await;
                pause.store(true, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(300)).await; // drain in-flight txns
                let Ok(row) = client
                    .query_one(
                        "SELECT pg_current_wal_lsn()::text, count(*), sum(amount)::float8 FROM app.orders",
                        &[],
                    )
                    .await
                else {
                    pause.store(false, Ordering::Relaxed);
                    continue;
                };
                let head: String = row.get(0);
                let src_count: i64 = row.get(1);
                let src_sum: f64 = row.get(2);
                let Ok(lsn) = parse_lsn(&head) else {
                    pause.store(false, Ordering::Relaxed);
                    continue;
                };
                let caught = engine.wait_applied(lsn, Duration::from_secs(20)).await;
                let verdict = if !caught {
                    (false, format!("apply never reached {head}"))
                } else {
                    match engine
                        .query(
                            "SELECT count(*) AS c, sum(CAST(amount AS DOUBLE)) AS s FROM app.orders",
                            &format!("target_lsn={head}"),
                        )
                        .await
                    {
                        Ok((rows, _, _)) if !rows.is_empty() => {
                            let g_count: i64 = rows[0][0].parse().unwrap_or(-1);
                            let g_sum: f64 = rows[0][1].parse().unwrap_or(f64::NAN);
                            let sum_ok = ((g_sum - src_sum) / src_sum.max(1.0)).abs() < 1e-9;
                            let ok = g_count == src_count && sum_ok;
                            (
                                ok,
                                format!(
                                    "@{head}: count src={src_count} gray={g_count}, sum src={src_sum:.2} gray={g_sum:.2}"
                                ),
                            )
                        }
                        Ok(_) => (false, "empty result".to_string()),
                        Err(e) => (false, format!("query failed: {e}")),
                    }
                };
                results.lock().await.push(verdict);
                pause.store(false, Ordering::Relaxed);
            }
        })
    };

    {
        let mut heavy_stats = PhaseStats::default();
        run_phase(&engine, phase_secs, &mut heavy_stats).await?;
        stats.lock().await.1.q1_ms = heavy_stats.q1_ms;
        stats.lock().await.1.q2_ms = heavy_stats.q2_ms;
    }

    stop.store(true, Ordering::Relaxed);
    let (ins, upd, del) = cdc_task.await.unwrap_or((0, 0, 0));
    probe_task.await.ok();
    fresh_task.await.ok();

    // ---- Report --------------------------------------------------------------------
    println!("\n[5/5] results");
    let s = stats.lock().await;
    println!("\n== R1-local: GrayDB column ==");
    println!("cdc applied to source: {ins} inserts, {upd} update-attempts, {del} delete-attempts");
    let mut json = serde_json::json!({
        "seed_rows": have, "tps": tps, "phase_secs": phase_secs,
        "build": if cfg!(debug_assertions) { "debug" } else { "release" },
        "postgres": version,
    });
    for (phase, ps) in [("quiet", &s.0), ("heavy", &s.1)] {
        println!("--- {phase} ---");
        for (name, v) in [("Q1 group-by-all", ps.q1_ms.clone()), ("Q2 filtered group-by", ps.q2_ms.clone()), ("freshness src->visible", ps.freshness_ms.clone())] {
            let (line, p50, p95, p99) = summarize(name, v);
            println!("  {line}");
            json[phase][name] = serde_json::json!({"p50_ms": p50, "p95_ms": p95, "p99_ms": p99});
        }
    }
    let probes = probe_results.lock().await;
    let mut all_ok = !probes.is_empty();
    println!("--- correctness probes (exact at measured source LSN) ---");
    for (ok, msg) in probes.iter() {
        all_ok &= ok;
        println!("  [{}] {msg}", if *ok { "PASS" } else { "FAIL" });
    }
    json["correctness_probes_pass"] = serde_json::json!(all_ok);

    let status = engine.status().await?;
    println!(
        "--- pipeline ---\n  frames={} commits={} applied={} received={}",
        status.frames, status.commits, status.applied_lsn, status.received_lsn
    );
    for t in &status.tables {
        println!("  {:<16} rows_visible={}", t.table, t.rows_visible);
    }

    std::fs::create_dir_all("bench-results").ok();
    let out = format!(
        "bench-results/r1-local-{}.json",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    );
    std::fs::write(&out, serde_json::to_vec_pretty(&json)?)?;
    println!("\nwritten: {out}");
    println!(
        "NOTE: {} — ClickHouse column requires the Linux stage (docs/RESEARCH-R1.md).",
        format_lsn(parse_lsn(&status.applied_lsn).unwrap_or(0))
    );

    anyhow::ensure!(all_ok, "correctness probes failed — numbers above are void");
    Ok(())
}
