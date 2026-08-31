//! Demo 2 (SP2): concurrent ingestion during load — the frame log consumes the slot
//! from LSN0 while the parallel COPY runs, acks only durably-synced txn-complete
//! prefixes, and graydb-check verifies custody (crc, seq contiguity, LSN monotonicity,
//! ack == durable == confirmed_flush) alongside the SP1 multiset check.
//! Demo 8 (SP2): WAL-budget ladder rungs 1–3 under an induced stall — pause the log
//! writer, watch the budget gauge climb through warn/shed while frames spill to
//! staging (rung 3), resume, watch staging drain and the ack catch up.
//! Run: `just demo-sp2` (pg17) or `just demo-sp2-pg16`.

use anyhow::{Context, Result};
use graydb_check::multiset;
use graydb_ingest::repl::{format_lsn, parse_lsn, ReplClient};
use graydb_ingest::stream::{IngestMetrics, PumpCommand};
use graydb_ingest::{attach, budget, config::Config, snapshot, stream};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

const SEED_SQL: &str = include_str!("../../../../db/seed.sql");
const MARKER: &str = "GRAYDB_SP2_MARKER";
/// Demo-scale WAL budget so the ladder trips in seconds, not hours
/// (override: GRAYDB_WAL_BUDGET_BYTES; production default stays in graydb.toml).
const DEMO_BUDGET_BYTES: u64 = 4 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::load()?;
    println!("== GrayDB SP2 : frame log + ack invariant (Demo 2) & WAL ladder (Demo 8) ==");
    println!(
        "source: {}:{}/{} publication={} slot={}",
        cfg.source.host, cfg.source.port, cfg.source.dbname,
        cfg.source.publication, cfg.source.slot
    );

    let admin = cfg.connect().await?;
    let version: String = admin.query_one("SHOW server_version", &[]).await?.get(0);
    println!("source version: {version}");

    // ---- Fresh demo state (SP1 groundwork) -------------------------------------
    attach::drop_slot_if_exists(&admin, &cfg.source.slot).await?;
    attach::drop_publication_if_exists(&admin, &cfg.source.publication).await?;
    println!("\n[1/8] seed + attach pack + publication ...");
    admin.batch_execute(SEED_SQL).await.context("seeding")?;
    attach::install_attach_pack(&admin).await?;
    admin.batch_execute("DELETE FROM graydb.ddl_log").await?;
    attach::ensure_publication(&admin, &cfg.source.publication, &cfg.source.schema).await?;

    // ---- Slot + exported snapshot (connection A holds the snapshot) ------------
    println!("[2/8] slot with exported snapshot ...");
    let mut repl_a = ReplClient::connect(
        &cfg.source.host, cfg.source.port, &cfg.source.user,
        &cfg.source.password, &cfg.source.dbname,
    ).await.context("replication connection A (snapshot holder)")?;
    let slot = repl_a.create_slot_with_snapshot(&cfg.source.slot).await?;
    let lsn0 = parse_lsn(&slot.consistent_point)?;
    println!("  LSN0={} snapshot={}", slot.consistent_point, slot.snapshot_name);

    // ---- Frame-log pump on connection B, streaming from LSN0 -------------------
    println!("[3/8] START_REPLICATION from LSN0 on a second session; pump -> frame log ...");
    let mut repl_b = ReplClient::connect(
        &cfg.source.host, cfg.source.port, &cfg.source.user,
        &cfg.source.password, &cfg.source.dbname,
    ).await.context("replication connection B (stream)")?;
    repl_b
        .start_replication(&cfg.source.slot, &cfg.source.publication, lsn0)
        .await?;

    let log_dir = cfg.storage.data_dir.join("log").join("sp2");
    let log = graydb_log::FrameLog::create(&log_dir, cfg.log.segment_max_bytes).await?;
    let durable_rx = log.durable();
    let metrics = Arc::new(IngestMetrics::default());
    let (ctrl_tx, ctrl_rx) = tokio::sync::watch::channel(PumpCommand::default());
    let pump = tokio::spawn(stream::run_pump(
        repl_b, log, lsn0, ctrl_rx, Arc::clone(&metrics),
    ));

    // ---- Demo 2: concurrent writes + parallel COPY at LSN0 ---------------------
    println!("[4/8] parallel COPY at LSN0 with concurrent writes streaming into the log ...");
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let writer = spawn_writer(cfg.clone(), stop_rx, 50, false);

    let snapshot_dir = cfg.storage.data_dir.join("snapshot").join("sp2");
    if snapshot_dir.exists() {
        tokio::fs::remove_dir_all(&snapshot_dir).await.ok();
    }
    let manifest =
        snapshot::run_parallel_copy(&cfg, &slot.consistent_point, &slot.snapshot_name, &snapshot_dir)
            .await?;

    println!("[5/8] graydb-check: multiset at LSN0 (SP1 invariant, still enforced) ...");
    let checker = cfg.connect().await?;
    snapshot::begin_snapshot_txn(&checker, &slot.snapshot_name).await?;
    let mut multiset_pass = true;
    for t in &manifest.tables {
        let c = multiset::check_table_at_snapshot(&checker, t, &snapshot_dir).await?;
        multiset_pass &= c.pass;
        println!(
            "  {:<24} source@LSN0={:>7} staged={:>7}  {}",
            c.table, c.source_rows, c.staged_rows,
            if c.pass { "PASS" } else { "FAIL" }
        );
    }
    checker.batch_execute("COMMIT").await.ok();
    repl_a.close().await.ok(); // snapshot released; stream B unaffected

    // Stop the writer, then land one marker transaction and wait for full custody.
    stop_tx.send(true).ok();
    let writer_txns = writer.await.unwrap_or(0);
    admin
        .execute(
            "INSERT INTO app.orders (customer_id, status, amount) VALUES (1, $1, 0.01)",
            &[&MARKER],
        )
        .await?;
    println!(
        "  {} writer txns during load; marker txn committed; waiting for durable custody ...",
        writer_txns
    );

    let mut demo2_custody = false;
    let mut confirmed_flush = 0u64;
    let mut durable = 0u64;
    for _ in 0..120 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let b = budget::sample(
            &admin, &cfg.source.slot, DEMO_BUDGET_BYTES,
            cfg.wal_budget.warn_fraction, cfg.wal_budget.shed_fraction,
        ).await?;
        confirmed_flush = b.confirmed_flush;
        durable = durable_rx.borrow().lsn;
        if !durable_rx.borrow().valid || durable != confirmed_flush {
            continue;
        }
        // Custody check: marker must be inside a checksummed durable frame.
        // Torn tail tolerated: the log is live (non-commit frames are unsynced).
        let mut marker_found = false;
        let v = graydb_log::verify_log(&log_dir, true, |f| {
            if !marker_found && f.payload.windows(MARKER.len()).any(|w| w == MARKER.as_bytes()) {
                marker_found = true;
            }
        })?;
        if marker_found && v.seq_contiguous && v.lsn_monotone {
            demo2_custody = true;
            break;
        }
    }
    let v2 = graydb_log::verify_log(&log_dir, true, |_| {})?;
    println!(
        "  frames={} commits={} seq_contiguous={} lsn_monotone={} durable={} confirmed_flush={}",
        v2.frames, v2.commits, v2.seq_contiguous, v2.lsn_monotone,
        format_lsn(durable), format_lsn(confirmed_flush)
    );
    let demo2_pass = demo2_custody && multiset_pass && v2.commits > writer_txns;

    // ---- Demo 8: induced stall -> rungs 1..3 -> resume --------------------------
    let budget_bytes = std::env::var("GRAYDB_WAL_BUDGET_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEMO_BUDGET_BYTES);
    println!(
        "\n[6/8] Demo 8: induced stall — pausing the log writer (demo budget {} KiB) ...",
        budget_bytes / 1024
    );
    // Settle first: restart_lsn advances lazily after phase A's burst; the gauge
    // must start below the warn line or the ladder walk is theater, not measurement.
    // restart_lsn only moves when the decoder passes a fresh standby-snapshot record,
    // so we force one per poll (pg_log_standby_snapshot, PG16+) plus a tiny commit
    // to carry confirmed_flush past it. Never silent on failure.
    let mut settled = false;
    for _ in 0..120 {
        admin.execute("SELECT pg_log_standby_snapshot()", &[]).await.ok();
        admin
            .execute(
                "INSERT INTO app.orders (customer_id, status, amount) VALUES (1, 'settle', 0.00)",
                &[],
            )
            .await
            .ok();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let b = budget::sample(
            &admin, &cfg.source.slot, budget_bytes,
            cfg.wal_budget.warn_fraction, cfg.wal_budget.shed_fraction,
        ).await?;
        if b.consumed_fraction < cfg.wal_budget.warn_fraction {
            println!(
                "  settled: retained {}K ({:.1}% of budget) before stall",
                b.retained_bytes / 1024, b.consumed_fraction * 100.0
            );
            settled = true;
            break;
        }
    }
    if !settled {
        println!("  WARNING: gauge did not settle below warn in 60s — ladder will start saturated");
    }
    let (stop_tx8, stop_rx8) = tokio::sync::watch::channel(false);
    let writer8 = spawn_writer(cfg.clone(), stop_rx8, 200, true);
    ctrl_tx.send(PumpCommand { stalled: true, shutdown: false })?;

    let mut warn_seen = false;
    let mut shed_seen = false;
    let mut spill_seen = false;
    println!("  {:>8}  {:>10}  {:>6}  {:>5}  {:>6}", "t(ms)", "retained", "used%", "rung", "spill");
    let t0 = std::time::Instant::now();
    for _ in 0..480 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let b = budget::sample(
            &admin, &cfg.source.slot, budget_bytes,
            cfg.wal_budget.warn_fraction, cfg.wal_budget.shed_fraction,
        ).await?;
        let spilled = metrics.spilled_frames.load(Ordering::Relaxed);
        spill_seen |= spilled > 0;
        warn_seen |= b.rung >= 1;
        shed_seen |= b.rung >= 2;
        println!(
            "  {:>8}  {:>9}K  {:>5.1}%  {:>5}  {:>6}",
            t0.elapsed().as_millis(),
            b.retained_bytes / 1024,
            b.consumed_fraction * 100.0,
            b.rung,
            spilled
        );
        // End the stall only once every rung has actually been exercised.
        if warn_seen && shed_seen && spill_seen && b.consumed_fraction >= 0.8 {
            break;
        }
    }

    println!("[7/8] resume: drain staging -> durable -> ack catches up ...");
    ctrl_tx.send(PumpCommand { stalled: false, shutdown: false })?;
    stop_tx8.send(true).ok();
    let _ = writer8.await;

    let mut recovered = false;
    for _ in 0..120 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let b = budget::sample(
            &admin, &cfg.source.slot, budget_bytes,
            cfg.wal_budget.warn_fraction, cfg.wal_budget.shed_fraction,
        ).await?;
        if b.consumed_fraction < cfg.wal_budget.warn_fraction {
            println!(
                "  retained back to {}K ({:.1}% of budget) — below warn threshold",
                b.retained_bytes / 1024,
                b.consumed_fraction * 100.0
            );
            recovered = true;
            break;
        }
    }

    // ---- Shutdown + final custody verification ----------------------------------
    println!("[8/8] shutdown pump; final log verification ...");
    ctrl_tx.send(PumpCommand { stalled: false, shutdown: true })?;
    pump.await.context("pump task")??;
    // Clean shutdown: a torn tail here would be a real defect, so none tolerated.
    let vf = graydb_log::verify_log(&log_dir, false, |_| {})?;
    let spilled_total = metrics.spilled_frames.load(Ordering::Relaxed);
    println!(
        "  final: frames={} commits={} seq_contiguous={} lsn_monotone={} spilled_frames={} max_lsn={}",
        vf.frames, vf.commits, vf.seq_contiguous, vf.lsn_monotone,
        spilled_total, format_lsn(vf.max_lsn_end)
    );

    println!("\n== SP2 verdict ==");
    println!("  DEMO 2 — multiset at LSN0           : {}", verdict(multiset_pass));
    println!("  DEMO 2 — durable ack == confirmed   : {}", verdict(demo2_custody));
    println!("  DEMO 2 — stream custody (crc/seq)   : {}", verdict(v2.seq_contiguous && v2.lsn_monotone));
    println!("  DEMO 8 — rung 1 (warn >=50%)        : {}", verdict(warn_seen));
    println!("  DEMO 8 — rung 2 (shed >=70%)        : {}", verdict(shed_seen));
    println!("  DEMO 8 — rung 3 (spill to staging)  : {}", verdict(spill_seen));
    println!("  DEMO 8 — recovery below warn        : {}", verdict(recovered));
    println!("  final log verification              : {}", verdict(vf.seq_contiguous && vf.lsn_monotone && vf.frames > 0));

    let all = demo2_pass
        && warn_seen && shed_seen && spill_seen && recovered
        && vf.seq_contiguous && vf.lsn_monotone;
    if all {
        println!("\nDEMO 2 + DEMO 8: PASS — ack never outran durability; ladder rungs 1–3 exercised; zero gap, zero dup.");
        Ok(())
    } else {
        anyhow::bail!("SP2 DEMO: FAIL — see verdict table");
    }
}

fn verdict(ok: bool) -> &'static str {
    if ok { "PASS" } else { "FAIL" }
}

/// Concurrent writer: one INSERT statement per txn. `bulky` pads rows (~0.5 KB each)
/// to generate WAL fast for the budget ladder.
fn spawn_writer(
    cfg: Config,
    stop: tokio::sync::watch::Receiver<bool>,
    rows_per_txn: i64,
    bulky: bool,
) -> tokio::task::JoinHandle<u64> {
    tokio::spawn(async move {
        let client = match cfg.connect().await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "writer failed to connect");
                return 0;
            }
        };
        let sql = if bulky {
            "INSERT INTO app.orders (customer_id, status, amount)
             SELECT 1 + (g % 5000), repeat('x', 500), 1.00 FROM generate_series(1, $1::bigint) g"
        } else {
            "INSERT INTO app.orders (customer_id, status, amount)
             SELECT 1 + (g % 5000), 'concurrent', 42.42 FROM generate_series(1, $1::bigint) g"
        };
        let mut txns = 0u64;
        loop {
            if *stop.borrow() {
                break;
            }
            match client.execute(sql, &[&rows_per_txn]).await {
                Ok(_) => txns += 1,
                Err(e) => {
                    tracing::warn!(error = %e, "writer insert failed");
                    break;
                }
            }
            // Bulky mode paces itself so the budget gauge walks visibly through the
            // rungs on a live call instead of jumping 0 -> 100% between samples.
            if bulky {
                tokio::time::sleep(Duration::from_millis(50)).await;
            } else if txns % 20 == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        txns
    })
}
