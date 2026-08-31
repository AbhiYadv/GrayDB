//! SP7 chaos: Demos 4 + 7 + source-failover simulation.
//!   Demo 7 — kill the decoder mid-stream (task abort, dying session, torn tail),
//!            then FRESH session from the last durable ack: FrameLog::resume
//!            truncates past the durable boundary, a new replication session makes
//!            Postgres re-emit Relation metadata, and the stream continues with
//!            contiguous seqs, zero gap, zero duplicate.
//!   Demo 4 — crash AFTER frames are durable but BEFORE materialization: rebuild
//!            everything from disk artifacts alone; row multisets must equal the
//!            live source exactly.
//!   Failover — source Postgres stopped with `-m immediate` (crash semantics) and
//!            restarted; another fresh session continues from the durable ack.
//! Run: `just demo-sp7` (pg17) or `just demo-sp7-pg16`.

use anyhow::{Context, Result};
use graydb_check::harness::{project_multiset, source_multiset};
use graydb_columnar::{ColumnSpec, TableStore};
use graydb_ingest::repl::{format_lsn, parse_lsn, ReplClient};
use graydb_ingest::stream::{IngestMetrics, PumpCommand};
use graydb_ingest::{attach, config::Config, snapshot, stream};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

const SEED_SQL: &str = include_str!("../../../../db/seed.sql");
const MARKER1: &str = "GRAYDB_SP7_MARKER_AFTER_KILL";
const MARKER2: &str = "GRAYDB_SP7_MARKER_AFTER_FAILOVER";

struct Pump {
    handle: tokio::task::JoinHandle<Result<()>>,
    ctrl: tokio::sync::watch::Sender<PumpCommand>,
    durable: tokio::sync::watch::Receiver<graydb_log::DurableMark>,
    metrics: Arc<IngestMetrics>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tantivy=warn".into()),
        )
        .init();

    let cfg = Config::load()?;
    println!("== GrayDB SP7 : chaos — decoder kill, crash-before-materialize, source failover ==");
    let admin = cfg.connect().await?;
    let version: String = admin.query_one("SHOW server_version", &[]).await?.get(0);
    println!(
        "source: {}:{}/{} ({version})",
        cfg.source.host, cfg.source.port, cfg.source.dbname
    );

    // ---- Setup: seed, slot, backfill, pump #1 -------------------------------------
    attach::drop_slot_if_exists(&admin, &cfg.source.slot).await?;
    attach::drop_publication_if_exists(&admin, &cfg.source.publication).await?;
    println!("\n[1/8] seed + attach + slot + backfill + pump #1 ...");
    admin.batch_execute(SEED_SQL).await.context("seeding")?;
    attach::install_attach_pack(&admin).await?;
    admin.batch_execute("DELETE FROM graydb.ddl_log").await?;
    attach::ensure_publication(&admin, &cfg.source.publication, &cfg.source.schema).await?;

    let mut repl_a = ReplClient::connect(
        &cfg.source.host, cfg.source.port, &cfg.source.user,
        &cfg.source.password, &cfg.source.dbname,
    ).await?;
    let slot = repl_a.create_slot_with_snapshot(&cfg.source.slot).await?;
    let lsn0 = parse_lsn(&slot.consistent_point)?;
    println!("  LSN0={}", slot.consistent_point);

    let log_dir = cfg.storage.data_dir.join("log").join("sp7");
    let log = graydb_log::FrameLog::create(&log_dir, cfg.log.segment_max_bytes).await?;
    let pump1 = start_pump(&cfg, log, lsn0).await?;

    let snapshot_dir = cfg.storage.data_dir.join("snapshot").join("sp7");
    if snapshot_dir.exists() {
        tokio::fs::remove_dir_all(&snapshot_dir).await.ok();
    }
    let manifest =
        snapshot::run_parallel_copy(&cfg, &slot.consistent_point, &slot.snapshot_name, &snapshot_dir)
            .await?;
    repl_a.close().await.ok();

    // ---- Demo 7: kill the decoder mid-stream ---------------------------------------
    println!("[2/8] Demo 7: writer running; killing the decoder MID-STREAM ...");
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let writer = spawn_writer(cfg.clone(), stop_rx, 50);
    // Wait until real traffic is flowing and partially durable.
    let mut waited = 0;
    while pump1.metrics.commits.load(Ordering::Relaxed) < 20 && waited < 240 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        waited += 1;
    }
    let durable_at_kill = pump1.durable.borrow().lsn;
    pump1.handle.abort(); // decoder death: dying session, no clean shutdown
    let _ = pump1.handle.await; // JoinError(cancelled) expected
    println!(
        "  decoder killed at durable={} (commits so far: {})",
        format_lsn(durable_at_kill),
        pump1.metrics.commits.load(Ordering::Relaxed)
    );
    // Source keeps moving while we are dead.
    tokio::time::sleep(Duration::from_millis(750)).await;
    stop_tx.send(true).ok();
    let writer_txns = writer.await.unwrap_or(0);
    println!("  writer wrote {writer_txns} txns total (source moved on while decoder was dead)");

    // ---- Fresh session from the durable ack -----------------------------------------
    println!("[3/8] fresh-session restart: resume log at durable boundary, START_REPLICATION from ack ...");
    let log = graydb_log::FrameLog::resume(&log_dir, cfg.log.segment_max_bytes).await?;
    let resumed_mark = log.durable_now();
    anyhow::ensure!(resumed_mark.valid, "resume found no durable boundary");
    println!(
        "  resumed: durable seq={} lsn={} (tail past boundary truncated)",
        resumed_mark.seq,
        format_lsn(resumed_mark.lsn)
    );
    let pump2 = start_pump(&cfg, log, resumed_mark.lsn).await?;

    admin
        .execute(
            "INSERT INTO app.orders (customer_id, status, amount) VALUES (1, $1, 0.01)",
            &[&MARKER1],
        )
        .await?;
    let head1 = current_lsn(&admin).await?;
    wait_durable(&pump2, head1).await?;
    println!("  caught up through {} on the fresh session", format_lsn(head1));

    // ---- Demo 4: crash after durable, BEFORE materialize -----------------------------
    println!("[4/8] Demo 4: crash before materialize (abort pump #2, nothing materialized yet) ...");
    pump2.handle.abort();
    let _ = pump2.handle.await;

    println!("[5/8] restart-materialize purely from disk artifacts ...");
    let log = graydb_log::FrameLog::resume(&log_dir, cfg.log.segment_max_bytes).await?;
    let mark_after_crash = log.durable_now();
    drop(log); // materialization reads segments; the next pump resumes later
    let replay = graydb_registry::replay_log(&log_dir)?;
    let stores = materialize(&cfg, &manifest, &snapshot_dir, lsn0, &replay, "sp7")?;

    let mut d4_equal = true;
    for (table, cols, sql_cols) in [
        ("app.orders", &[0usize, 1, 2, 3][..], "id, customer_id, status, amount"),
        ("app.customers", &[0usize, 1, 2, 3][..], "id, name, email, balance"),
        ("app.notes", &[0usize][..], "body"),
    ] {
        let src = source_multiset(&admin, table, sql_cols).await?;
        let ours = project_multiset(stores.get(table).context("store")?.scan_at(u64::MAX)?, cols);
        let ok = src == ours;
        d4_equal &= ok;
        println!(
            "  {:<16} source={:<6} rebuilt={:<6} {}",
            table, src.len(), ours.len(),
            if ok { "PASS" } else { "FAIL" }
        );
    }
    let marker1_count = replay
        .changes
        .iter()
        .filter(|c| {
            c.new.as_ref().is_some_and(|n| n.iter().any(|(k, v)| {
                k == "status"
                    && matches!(v, graydb_registry::pgoutput::TupleValue::Text(s) if s == MARKER1)
            }))
        })
        .count();
    let verification = graydb_log::verify_log(&log_dir, false, |_| {})?;
    let orders_versions = replay
        .registry
        .tables
        .values()
        .find(|t| t.qualified_name == "app.orders")
        .map(|t| t.versions.len())
        .unwrap_or(0);
    println!(
        "  marker1 count={marker1_count} (expect 1)  seq_contiguous={} lsn_monotone={}  registry versions(app.orders)={orders_versions} across 2 sessions",
        verification.seq_contiguous, verification.lsn_monotone
    );
    let d7_ok = marker1_count == 1
        && verification.seq_contiguous
        && verification.lsn_monotone
        && mark_after_crash.valid
        && orders_versions == 1;

    // ---- Source failover simulation ----------------------------------------------------
    println!("[6/8] source failover: pg_ctl -m immediate stop, then start ...");
    let (bin, data) = pg_paths(&cfg)?;
    run_pg_ctl(&bin, &data, &["-m", "immediate", "-w", "stop"])?;
    run_pg_ctl(&bin, &data, &["-w", "start"])?;
    let admin2 = wait_for_source(&cfg).await?;
    println!("  source restarted (crash semantics) and accepting connections");

    println!("[7/8] fresh session #3 from the durable ack; continue streaming ...");
    let log = graydb_log::FrameLog::resume(&log_dir, cfg.log.segment_max_bytes).await?;
    let mark = log.durable_now();
    let pump3 = start_pump(&cfg, log, mark.lsn).await?;
    admin2
        .batch_execute(
            "INSERT INTO app.orders (customer_id, status, amount)
             SELECT 1 + (g % 5000), 'post-failover', 1.00 FROM generate_series(1, 200) g",
        )
        .await?;
    admin2
        .execute(
            "INSERT INTO app.orders (customer_id, status, amount) VALUES (1, $1, 0.01)",
            &[&MARKER2],
        )
        .await?;
    let head2 = current_lsn(&admin2).await?;
    wait_durable(&pump3, head2).await?;
    pump3.ctrl.send(PumpCommand { stalled: false, shutdown: true })?;
    pump3.handle.await.context("pump3")??;

    let replay2 = graydb_registry::replay_log(&log_dir)?;
    let stores2 = materialize(&cfg, &manifest, &snapshot_dir, lsn0, &replay2, "sp7-final")?;
    let src = source_multiset(&admin2, "app.orders", "id, customer_id, status, amount").await?;
    let ours = project_multiset(
        stores2.get("app.orders").context("orders")?.scan_at(u64::MAX)?,
        &[0, 1, 2, 3],
    );
    let marker2_count = replay2
        .changes
        .iter()
        .filter(|c| {
            c.new.as_ref().is_some_and(|n| n.iter().any(|(k, v)| {
                k == "status"
                    && matches!(v, graydb_registry::pgoutput::TupleValue::Text(s) if s == MARKER2)
            }))
        })
        .count();
    let vfinal = graydb_log::verify_log(&log_dir, false, |_| {})?;
    let failover_ok = src == ours && marker2_count == 1 && vfinal.seq_contiguous && vfinal.lsn_monotone;
    println!(
        "  post-failover: orders source={} rebuilt={} equal={}  marker2={marker2_count} (expect 1)",
        src.len(), ours.len(), src == ours
    );

    println!("\n[8/8] verdict");
    println!("\n== SP7 verdict ==");
    println!("  DEMO 7 — fresh session from durable ack, zero gap/dup: {}", verdict(d7_ok));
    println!("  DEMO 4 — crash-before-materialize replay equality    : {}", verdict(d4_equal));
    println!("  failover — continuity across source crash-restart    : {}", verdict(failover_ok));
    println!("  frame log final verification (3 sessions, 1 log)     : {}", verdict(vfinal.seq_contiguous && vfinal.lsn_monotone));

    if d7_ok && d4_equal && failover_ok {
        println!("\nDEMOS 4 + 7 + FAILOVER: PASS — kill anything; the log's durable prefix is the truth and it never lies.");
        Ok(())
    } else {
        anyhow::bail!("SP7 DEMO: FAIL — see verdict table");
    }
}

fn verdict(ok: bool) -> &'static str {
    if ok { "PASS" } else { "FAIL" }
}

async fn start_pump(cfg: &Config, log: graydb_log::FrameLog, start_lsn: u64) -> Result<Pump> {
    // The old session may linger server-side briefly after an abort: retry acquisition.
    let mut attempt = 0;
    loop {
        let mut repl = ReplClient::connect(
            &cfg.source.host, cfg.source.port, &cfg.source.user,
            &cfg.source.password, &cfg.source.dbname,
        ).await?;
        match repl.start_replication(&cfg.source.slot, &cfg.source.publication, start_lsn).await {
            Ok(()) => {
                let durable = log.durable();
                let metrics = Arc::new(IngestMetrics::default());
                let (ctrl, ctrl_rx) = tokio::sync::watch::channel(PumpCommand::default());
                let handle = tokio::spawn(stream::run_pump(
                    repl, log, start_lsn, ctrl_rx, Arc::clone(&metrics),
                ));
                return Ok(Pump { handle, ctrl, durable, metrics });
            }
            Err(e) if attempt < 40 && e.to_string().contains("active") => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

async fn wait_durable(pump: &Pump, lsn: u64) -> Result<()> {
    for _ in 0..240 {
        if pump.durable.borrow().valid && pump.durable.borrow().lsn >= lsn {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    anyhow::bail!("pump did not reach {}", format_lsn(lsn))
}

fn materialize(
    cfg: &Config,
    manifest: &graydb_ingest::snapshot::SnapshotManifest,
    snapshot_dir: &PathBuf,
    lsn0: u64,
    replay: &graydb_registry::Replay,
    tag: &str,
) -> Result<HashMap<String, TableStore>> {
    let root = cfg.storage.data_dir.join("columnar").join(tag);
    let mut stores: HashMap<String, TableStore> = HashMap::new();
    for t in &manifest.tables {
        let specs: Vec<ColumnSpec> = t
            .columns
            .iter()
            .zip(t.column_oids.iter())
            .map(|(name, oid)| ColumnSpec {
                name: name.clone(),
                type_oid: *oid,
                is_key: t.key_columns.contains(name),
            })
            .collect();
        let mut store =
            TableStore::create(&root.join(&t.table), &t.table, specs, cfg.columnar.flush_rows)?;
        for part in &t.parts {
            let data = std::fs::read(snapshot_dir.join(&part.file))?;
            store.load_copy_part(&data, lsn0)?;
        }
        store.flush()?;
        stores.insert(t.table.clone(), store);
    }
    for change in &replay.changes {
        if let Some(store) = stores.get_mut(&change.table) {
            store.apply(change)?;
        } else if change.table != "graydb.ddl_log" {
            anyhow::bail!("change for unknown table {}", change.table);
        }
    }
    for store in stores.values_mut() {
        store.finalize()?;
    }
    Ok(stores)
}

fn pg_paths(cfg: &Config) -> Result<(PathBuf, PathBuf)> {
    let v = match cfg.source.port {
        5416 => "pg16",
        5417 => "pg17",
        p => anyhow::bail!("no local pg_ctl mapping for port {p}"),
    };
    let tools = std::path::Path::new("..").join("tools");
    Ok((
        tools.join(v).join("pgsql").join("bin").join("pg_ctl.exe"),
        tools.join("pgdata").join(v),
    ))
}

fn run_pg_ctl(bin: &PathBuf, data: &PathBuf, args: &[&str]) -> Result<()> {
    let mut cmd = std::process::Command::new(bin);
    cmd.arg("-D").arg(data);
    for a in args {
        cmd.arg(a);
    }
    if args.contains(&"start") {
        cmd.arg("-l").arg(data.join("server.log"));
    }
    // Null stdio, always: a started postgres.exe inherits our handles otherwise, and
    // any pipe capturing THIS demo's output then never sees EOF (11-hour lesson).
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let status = cmd.status().with_context(|| format!("running {}", bin.display()))?;
    anyhow::ensure!(status.success(), "pg_ctl {:?} failed with {status}", args);
    Ok(())
}

async fn wait_for_source(cfg: &Config) -> Result<tokio_postgres::Client> {
    for attempt in 1..=120 {
        match cfg.connect().await {
            Ok(c) => return Ok(c),
            Err(e) if attempt == 120 => return Err(e).context("source never came back"),
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    unreachable!()
}

async fn current_lsn(admin: &tokio_postgres::Client) -> Result<u64> {
    let s: String = admin
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    parse_lsn(&s)
}

fn spawn_writer(
    cfg: Config,
    stop: tokio::sync::watch::Receiver<bool>,
    rows_per_txn: i64,
) -> tokio::task::JoinHandle<u64> {
    tokio::spawn(async move {
        let client = match cfg.connect().await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "writer failed to connect");
                return 0;
            }
        };
        let mut txns = 0u64;
        loop {
            if *stop.borrow() {
                break;
            }
            match client
                .execute(
                    "INSERT INTO app.orders (customer_id, status, amount)
                     SELECT 1 + (g % 5000), 'chaos', 9.99 FROM generate_series(1, $1::bigint) g",
                    &[&rows_per_txn],
                )
                .await
            {
                Ok(_) => txns += 1,
                Err(e) => {
                    tracing::warn!(error = %e, "writer insert failed");
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        txns
    })
}
