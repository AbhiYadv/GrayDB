//! Demo 1 (SP1): exported-snapshot initial load.
//! Proves: attach pack installs (SQL objects only), slot created with exported snapshot,
//! parallel ctid-range COPY lands the schema EXACTLY at LSN0 while concurrent writes
//! keep hitting the source, graydb-check confirms row-multiset equality at LSN0,
//! and the event-trigger ddl_log captures a live DDL.
//! Run: `just demo-sp1` (pg17) or `just demo-sp1-pg16`.

use anyhow::{Context, Result};
use graydb_check::multiset;
use graydb_ingest::{attach, config::Config, repl::ReplClient, snapshot};
use std::time::Duration;

const SEED_SQL: &str = include_str!("../../../../db/seed.sql");

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::load()?;
    println!("== GrayDB SP1 / Demo 1 : exported-snapshot initial load ==");
    println!(
        "source: {}:{}/{} schema={} publication={} slot={}",
        cfg.source.host,
        cfg.source.port,
        cfg.source.dbname,
        cfg.source.schema,
        cfg.source.publication,
        cfg.source.slot
    );

    // 0. Wait for the source, then start from a clean demo state.
    let admin = wait_for_source(&cfg).await?;
    let version: String = admin
        .query_one("SHOW server_version", &[])
        .await?
        .get(0);
    let wal_level: String = admin.query_one("SHOW wal_level", &[]).await?.get(0);
    println!("source version: {version} (wal_level={wal_level})");
    anyhow::ensure!(wal_level == "logical", "wal_level must be logical");

    attach::drop_slot_if_exists(&admin, &cfg.source.slot).await?;
    attach::drop_publication_if_exists(&admin, &cfg.source.publication).await?;

    // 1. Seed the demo schema (drops + recreates app.*).
    println!("\n[1/6] seeding schema '{}' ...", cfg.source.schema);
    admin.batch_execute(SEED_SQL).await.context("seeding")?;

    // 2. Attach: SQL-objects-only footprint (I5) + publication + eligibility report.
    println!("[2/6] installing attach pack (publication + event-trigger ddl_log) ...");
    attach::install_attach_pack(&admin).await?;
    admin
        .batch_execute("DELETE FROM graydb.ddl_log")
        .await
        .context("clearing ddl_log for a clean demo window")?;
    attach::ensure_publication(&admin, &cfg.source.publication, &cfg.source.schema).await?;

    let eligibility = attach::eligibility_scan(&admin, &cfg.source.schema).await?;
    println!("\n  table eligibility (Amendment A A5.1):");
    println!("  {:<24} {:<10} {:<8} {}", "table", "replident", "pk", "eligibility");
    for e in &eligibility {
        println!(
            "  {:<24} {:<10} {:<8} {}",
            e.table,
            (e.replident as u8) as char,
            e.has_pk,
            e.eligibility
        );
    }

    // 3. Replication session: create slot with exported snapshot. LSN0 is born here.
    println!("\n[3/6] creating replication slot with exported snapshot ...");
    let mut repl = ReplClient::connect(
        &cfg.source.host,
        cfg.source.port,
        &cfg.source.user,
        &cfg.source.password,
        &cfg.source.dbname,
    )
    .await
    .context("replication connection")?;
    let (systemid, timeline, xlogpos, _) = repl.identify_system().await?;
    println!("  IDENTIFY_SYSTEM: systemid={systemid} timeline={timeline} head={xlogpos}");
    let slot = repl.create_slot_with_snapshot(&cfg.source.slot).await?;
    println!(
        "  slot={} plugin={} LSN0={} snapshot={}",
        slot.slot_name, slot.output_plugin, slot.consistent_point, slot.snapshot_name
    );

    // 4. Concurrent writer starts BEFORE the copy: the load must exclude every one
    //    of these post-LSN0 writes or the invariant is broken.
    println!("\n[4/6] parallel COPY at LSN0 with concurrent writes hitting the source ...");
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let writer_cfg = cfg.clone();
    let writer = tokio::spawn(async move {
        let client = match writer_cfg.connect().await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "concurrent writer failed to connect");
                return 0u64;
            }
        };
        let mut inserted = 0u64;
        let mut n = 0i64;
        loop {
            if *stop_rx.borrow() {
                break;
            }
            n += 1;
            let res = client
                .execute(
                    "INSERT INTO app.orders (customer_id, status, amount)
                     SELECT 1 + (g % 5000), 'concurrent', 42.42
                     FROM generate_series(1, 50) g",
                    &[],
                )
                .await;
            match res {
                Ok(rows) => inserted += rows,
                Err(e) => {
                    tracing::warn!(error = %e, "concurrent insert failed");
                    break;
                }
            }
            if n % 10 == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        inserted
    });

    let snapshot_dir = cfg.storage.data_dir.join("snapshot").join("sp1");
    if snapshot_dir.exists() {
        tokio::fs::remove_dir_all(&snapshot_dir).await.ok();
    }
    let started = std::time::Instant::now();
    let manifest = snapshot::run_parallel_copy(
        &cfg,
        &slot.consistent_point,
        &slot.snapshot_name,
        &snapshot_dir,
    )
    .await?;
    let copy_elapsed = started.elapsed();

    stop_tx.send(true).ok();
    let concurrent_rows = writer.await.unwrap_or(0);
    let staged_rows: u64 = manifest.tables.iter().map(|t| t.rows).sum();
    let staged_bytes: u64 = manifest.tables.iter().map(|t| t.bytes).sum();
    println!(
        "  staged {} rows / {} bytes in {:.2?} across {} tables ({} parts); {} rows written concurrently",
        staged_rows,
        staged_bytes,
        copy_elapsed,
        manifest.tables.len(),
        manifest.tables.iter().map(|t| t.parts.len()).sum::<usize>(),
        concurrent_rows
    );

    // 5. graydb-check: row-multiset equality at LSN0, against the SAME exported snapshot.
    println!("\n[5/6] graydb-check: Materialized(table, LSN0) == SourceSnapshot(table, LSN0) ?");
    let checker = cfg.connect().await?;
    snapshot::begin_snapshot_txn(&checker, &slot.snapshot_name).await?;
    let mut all_pass = true;
    println!(
        "  {:<24} {:>12} {:>12}  {:<7} hash",
        "table", "source@LSN0", "staged", "verdict"
    );
    for t in &manifest.tables {
        let check = multiset::check_table_at_snapshot(&checker, t, &snapshot_dir).await?;
        all_pass &= check.pass;
        println!(
            "  {:<24} {:>12} {:>12}  {:<7} {}",
            check.table,
            check.source_rows,
            check.staged_rows,
            if check.pass { "PASS" } else { "FAIL" },
            &check.source_hash[..16]
        );
    }
    checker.batch_execute("COMMIT").await.ok();

    // The moment: the source has moved on; the staged load hasn't. Both are correct.
    let now_orders: i64 = admin
        .query_one("SELECT count(*) FROM app.orders", &[])
        .await?
        .get(0);
    let snap_orders = manifest
        .tables
        .iter()
        .find(|t| t.table.ends_with(".orders"))
        .map(|t| t.rows)
        .unwrap_or(0);
    println!(
        "\n  source app.orders NOW: {now_orders} rows; staged at LSN0: {snap_orders} rows \
         (delta = concurrent writes correctly excluded from the LSN0 load)"
    );
    let moved = now_orders as u64 > snap_orders;

    // 6. Live DDL through the event-trigger pack.
    println!("[6/6] live DDL capture via graydb.ddl_log ...");
    admin
        .batch_execute("ALTER TABLE app.customers ADD COLUMN demo_flag boolean DEFAULT false")
        .await?;
    let ddl_rows = admin
        .query(
            "SELECT kind, command_tag, object_identity, ddl_text
             FROM graydb.ddl_log ORDER BY id",
            &[],
        )
        .await?;
    for r in &ddl_rows {
        let kind: String = r.get(0);
        let tag: Option<String> = r.get(1);
        let ident: Option<String> = r.get(2);
        println!(
            "  ddl_log: [{kind}] {} {}",
            tag.unwrap_or_default(),
            ident.unwrap_or_default()
        );
    }
    let ddl_captured = !ddl_rows.is_empty();

    // Release the exported snapshot only after the checks are done.
    repl.close().await.ok();

    println!("\n== SP1 verdict ==");
    println!("  multiset equality at LSN0 : {}", verdict(all_pass));
    println!("  concurrent writes excluded: {}", verdict(moved));
    println!("  ddl_log capture           : {}", verdict(ddl_captured));
    println!("  LSN0 = {}  (recorded in {}/manifest.json)", manifest.lsn0, snapshot_dir.display());
    if all_pass && moved && ddl_captured {
        println!("\nDEMO 1: PASS — initial load == SourceSnapshot(LSN0), zero loss, zero contamination.");
        Ok(())
    } else {
        anyhow::bail!("DEMO 1: FAIL — see table above");
    }
}

fn verdict(ok: bool) -> &'static str {
    if ok {
        "PASS"
    } else {
        "FAIL"
    }
}

async fn wait_for_source(cfg: &Config) -> Result<tokio_postgres::Client> {
    for attempt in 1..=60 {
        match cfg.connect().await {
            Ok(c) => return Ok(c),
            Err(e) if attempt == 60 => return Err(e).context("source never became ready"),
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    unreachable!()
}
