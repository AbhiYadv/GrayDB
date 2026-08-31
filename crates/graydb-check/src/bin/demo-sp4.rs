//! Demo 3 (SP4): update + delete via replica identity land correctly in the columnar
//! store. Backfill (SP1 staged parts) becomes the base segment at LSN0; the frame log
//! replays post-LSN0 inserts/updates/deletes (updates hitting BOTH backfill rows and
//! streamed rows); graydb-check then proves:
//!   head state == source's current state (multiset), and
//!   target-LSN time travel: old versions visible at old LSNs, tombstones honored.
//! Run: `just demo-sp4` (pg17) or `just demo-sp4-pg16`.

use anyhow::{Context, Result};
use graydb_columnar::{copytext, ColumnSpec, TableStore};
use graydb_ingest::repl::{format_lsn, parse_lsn, ReplClient};
use graydb_ingest::stream::{IngestMetrics, PumpCommand};
use graydb_ingest::{attach, config::Config, snapshot, stream};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const SEED_SQL: &str = include_str!("../../../../db/seed.sql");
/// Compared columns for app.orders (exact text-render-safe: int8, int8, text, numeric).
/// created_at is excluded: timestamptz rendering across walsender vs COPY sessions is
/// a named-untested surface (MILESTONES SP4).
const ORDERS_CMP: [usize; 4] = [0, 1, 2, 3];

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::load()?;
    println!("== GrayDB SP4 / Demo 3 : update+delete via replica identity -> columnar ==");
    let admin = cfg.connect().await?;
    let version: String = admin.query_one("SHOW server_version", &[]).await?.get(0);
    println!(
        "source: {}:{}/{} ({version})",
        cfg.source.host, cfg.source.port, cfg.source.dbname
    );

    // ---- Fresh state, slot, pump, backfill --------------------------------------
    attach::drop_slot_if_exists(&admin, &cfg.source.slot).await?;
    attach::drop_publication_if_exists(&admin, &cfg.source.publication).await?;
    println!("\n[1/7] seed + attach + slot + pump + parallel COPY at LSN0 ...");
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

    let mut repl_b = ReplClient::connect(
        &cfg.source.host, cfg.source.port, &cfg.source.user,
        &cfg.source.password, &cfg.source.dbname,
    ).await?;
    repl_b.start_replication(&cfg.source.slot, &cfg.source.publication, lsn0).await?;
    let log_dir = cfg.storage.data_dir.join("log").join("sp4");
    let log = graydb_log::FrameLog::create(&log_dir, cfg.log.segment_max_bytes).await?;
    let durable_rx = log.durable();
    let metrics = Arc::new(IngestMetrics::default());
    let (ctrl_tx, ctrl_rx) = tokio::sync::watch::channel(PumpCommand::default());
    let pump = tokio::spawn(stream::run_pump(repl_b, log, lsn0, ctrl_rx, Arc::clone(&metrics)));

    let snapshot_dir = cfg.storage.data_dir.join("snapshot").join("sp4");
    if snapshot_dir.exists() {
        tokio::fs::remove_dir_all(&snapshot_dir).await.ok();
    }
    let manifest =
        snapshot::run_parallel_copy(&cfg, &slot.consistent_point, &slot.snapshot_name, &snapshot_dir)
            .await?;
    repl_a.close().await.ok();

    // ---- Post-LSN0 workload: inserts, updates (backfill + streamed rows), deletes --
    println!("[2/7] post-LSN0 workload: 2000 inserts, 3 update/delete waves ...");
    admin
        .batch_execute(
            "INSERT INTO app.orders (customer_id, status, amount)
             SELECT 1 + (g % 5000), 'streamed', (g % 999)::numeric / 10
             FROM generate_series(1, 2000) g",
        )
        .await?;
    let l1 = current_lsn(&admin).await?; // after inserts, before any update

    let updated: i64 = admin
        .execute(
            "UPDATE app.orders SET status = 'reprocessed', amount = amount + 1 WHERE id % 97 = 0",
            &[],
        )
        .await? as i64;
    let l2 = current_lsn(&admin).await?; // after wave 1 updates

    let deleted: i64 = admin
        .execute(
            "DELETE FROM app.orders WHERE id % 131 = 0 AND id % 97 <> 0",
            &[],
        )
        .await? as i64;
    let updated2: i64 = admin
        .execute(
            "UPDATE app.orders SET status = 're-reprocessed' WHERE id % 291 = 0",
            &[],
        )
        .await? as i64;
    let head_lsn = current_lsn(&admin).await?;
    println!("  updated={updated} deleted={deleted} re-updated={updated2}");

    // ---- Custody, shutdown, replay ------------------------------------------------
    println!("[3/7] waiting for durable custody, then pump shutdown ...");
    let mut caught_up = false;
    for _ in 0..240 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let m = *durable_rx.borrow();
        if m.valid && m.lsn >= head_lsn {
            caught_up = true;
            break;
        }
    }
    anyhow::ensure!(caught_up, "pump did not reach {}", format_lsn(head_lsn));
    ctrl_tx.send(PumpCommand { stalled: false, shutdown: true })?;
    pump.await.context("pump task")??;
    let replay = graydb_registry::replay_log(&log_dir)?;
    println!(
        "  replayed {} frames / {} txns / {} typed changes",
        replay.frames, replay.txns, replay.changes.len()
    );

    // ---- Build columnar stores: backfill base + streamed changes -------------------
    println!("[4/7] building columnar stores (backfill -> segment 0, then apply stream) ...");
    let col_root = cfg.storage.data_dir.join("columnar").join("sp4");
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
        let mut store = TableStore::create(
            &col_root.join(&t.table),
            &t.table,
            specs,
            cfg.columnar.flush_rows,
        )?;
        let mut loaded = 0u64;
        for part in &t.parts {
            let data = std::fs::read(snapshot_dir.join(&part.file))?;
            loaded += store.load_copy_part(&data, lsn0)?;
        }
        store.flush()?; // backfill becomes a FLUSHED segment: deletes must hit sidecars
        println!("  {:<16} backfill rows={loaded}", t.table);
        stores.insert(t.table.clone(), store);
    }

    let apply_started = std::time::Instant::now();
    let mut applied = 0u64;
    let mut skipped_ddl_log = 0u64;
    for change in &replay.changes {
        match stores.get_mut(&change.table) {
            Some(store) => {
                store.apply(change)?;
                applied += 1;
            }
            None if change.table == "graydb.ddl_log" => skipped_ddl_log += 1,
            None => anyhow::bail!("change for unknown table {}", change.table),
        }
    }
    let apply_elapsed = apply_started.elapsed();
    for store in stores.values_mut() {
        store.finalize()?;
    }
    println!(
        "  applied {applied} changes in {:.2?} ({:.0} changes/s, dev build) ; ddl_log rows skipped: {skipped_ddl_log}",
        apply_elapsed,
        applied as f64 / apply_elapsed.as_secs_f64().max(1e-9)
    );

    // ---- Check 1: head equality vs source current state ----------------------------
    println!("[5/7] graydb-check: head state == source current state (multiset) ...");
    let mut head_ok = true;
    for (table, cmp_cols, sql_cols) in [
        ("app.orders", &ORDERS_CMP[..], "id, customer_id, status, amount"),
        ("app.customers", &[0usize, 1, 2, 3][..], "id, name, email, balance"),
        ("app.notes", &[0usize][..], "body"),
    ] {
        let source = source_multiset(&admin, table, sql_cols).await?;
        let store = stores.get(table).context("store missing")?;
        let ours = project_multiset(store.scan_at(u64::MAX)?, cmp_cols);
        let ok = source == ours;
        head_ok &= ok;
        println!(
            "  {:<16} source_rows={:<6} store_rows={:<6} {}",
            table,
            source.len(),
            ours.len(),
            if ok { "PASS" } else { "FAIL" }
        );
    }

    // ---- Check 2: target-LSN time travel -------------------------------------------
    println!("[6/7] graydb-check: target-LSN time travel on app.orders ...");
    let orders = stores.get("app.orders").context("orders store")?;
    let at_l0 = orders.scan_at(lsn0)?;
    let at_l1 = orders.scan_at(l1)?;
    let at_l2 = orders.scan_at(l2)?;
    let at_head = orders.scan_at(u64::MAX)?;
    let backfill_rows = manifest
        .tables
        .iter()
        .find(|t| t.table == "app.orders")
        .map(|t| t.rows)
        .unwrap_or(0);
    let count_l0_ok = at_l0.len() as u64 == backfill_rows;
    let count_l1_ok = at_l1.len() as u64 == backfill_rows + 2000;

    let status_of = |rows: &[Vec<Option<String>>], id: &str| -> Option<String> {
        rows.iter()
            .find(|r| r[0].as_deref() == Some(id))
            .and_then(|r| r[2].clone())
    };
    // id=97: updated in wave 1 only. id=131: deleted. id=291: updated twice.
    let updated_ok = status_of(&at_l1, "97").as_deref() != Some("reprocessed")
        && status_of(&at_l2, "97").as_deref() == Some("reprocessed")
        && status_of(&at_head, "97").as_deref() == Some("reprocessed");
    let deleted_ok =
        status_of(&at_l1, "131").is_some() && status_of(&at_head, "131").is_none();
    let twice_ok = status_of(&at_l2, "291").as_deref() == Some("reprocessed")
        && status_of(&at_head, "291").as_deref() == Some("re-reprocessed");
    println!(
        "  rows@LSN0={} (expect {backfill_rows})  rows@L1={} (expect {})",
        at_l0.len(),
        at_l1.len(),
        backfill_rows + 2000
    );
    println!(
        "  id=97  L1:{:?} L2:{:?} head:{:?}",
        status_of(&at_l1, "97"), status_of(&at_l2, "97"), status_of(&at_head, "97")
    );
    println!(
        "  id=131 L1:{:?} head:{:?} (deleted)",
        status_of(&at_l1, "131"), status_of(&at_head, "131")
    );
    println!(
        "  id=291 L2:{:?} head:{:?} (updated twice)",
        status_of(&at_l2, "291"), status_of(&at_head, "291")
    );

    println!("\n[7/7] verdict");
    println!("\n== SP4 verdict ==");
    println!("  head multiset == source (3 tables) : {}", verdict(head_ok));
    println!("  row counts at LSN0 / L1            : {}", verdict(count_l0_ok && count_l1_ok));
    println!("  update via replica identity        : {}", verdict(updated_ok));
    println!("  delete via replica identity        : {}", verdict(deleted_ok));
    println!("  update-of-update (version chain)   : {}", verdict(twice_ok));

    if head_ok && count_l0_ok && count_l1_ok && updated_ok && deleted_ok && twice_ok {
        println!("\nDEMO 3: PASS — updates + deletes land correctly; every LSN answers with its own truth.");
        Ok(())
    } else {
        anyhow::bail!("DEMO 3: FAIL — see verdict table");
    }
}

fn verdict(ok: bool) -> &'static str {
    if ok { "PASS" } else { "FAIL" }
}

async fn current_lsn(admin: &tokio_postgres::Client) -> Result<u64> {
    let s: String = admin
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    parse_lsn(&s)
}

/// Current source rows for `table`, selected columns, as a sorted multiset of
/// tab-joined raw values (COPY text unescaped so both sides are raw renderings).
async fn source_multiset(
    admin: &tokio_postgres::Client,
    table: &str,
    cols: &str,
) -> Result<Vec<String>> {
    use futures_util::TryStreamExt;
    let sql = format!("COPY (SELECT {cols} FROM {table}) TO STDOUT");
    let stream = admin.copy_out(&sql).await?;
    futures_util::pin_mut!(stream);
    let mut data: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.try_next().await? {
        data.extend_from_slice(&chunk);
    }
    let mut out: Vec<String> = Vec::new();
    for line in data.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let vals = copytext::parse_copy_line(line);
        out.push(join_vals(&vals.iter().map(|v| v.as_deref()).collect::<Vec<_>>()));
    }
    out.sort_unstable();
    Ok(out)
}

fn project_multiset(rows: Vec<Vec<Option<String>>>, cols: &[usize]) -> Vec<String> {
    let mut out: Vec<String> = rows
        .iter()
        .map(|r| {
            let picked: Vec<Option<&str>> = cols.iter().map(|&i| r[i].as_deref()).collect();
            join_vals(&picked)
        })
        .collect();
    out.sort_unstable();
    out
}

fn join_vals(vals: &[Option<&str>]) -> String {
    vals.iter()
        .map(|v| v.unwrap_or("\u{0}NULL"))
        .collect::<Vec<_>>()
        .join("\t")
}
