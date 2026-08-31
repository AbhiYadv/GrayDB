//! Demo 5 (SP6): a caller-supplied target-LSN query returns the exact historical
//! answer — SQL over BOTH shapes through the reader, with the LSN proof on every
//! result. Also exercises search(table, query) as a table function joined to
//! columnar data, and graydb.stat_replication (D-014).
//! Run: `just demo-sp6` (pg17) or `just demo-sp6-pg16`.

use anyhow::{Context, Result};
use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use graydb_columnar::{copytext, ColumnSpec, TableStore};
use graydb_ingest::repl::{format_lsn, parse_lsn, ReplClient};
use graydb_ingest::stream::{IngestMetrics, PumpCommand};
use graydb_ingest::{attach, config::Config, snapshot, stream};
use graydb_search::SearchStore;
use graydb_studio::{Reader, TableShape};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const SEED_SQL: &str = include_str!("../../../../db/seed.sql");

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tantivy=warn".into()),
        )
        .init();

    let cfg = Config::load()?;
    println!("== GrayDB SP6 / Demo 5 : target-LSN reader over both shapes ==");
    let admin = cfg.connect().await?;
    let version: String = admin.query_one("SHOW server_version", &[]).await?.get(0);
    println!(
        "source: {}:{}/{} ({version})",
        cfg.source.host, cfg.source.port, cfg.source.dbname
    );

    // ---- Pipeline: seed, slot, pump, backfill, workload (SP4-style eras) ---------
    attach::drop_slot_if_exists(&admin, &cfg.source.slot).await?;
    attach::drop_publication_if_exists(&admin, &cfg.source.publication).await?;
    println!("\n[1/6] seed + attach + slot + pump + parallel COPY at LSN0 ...");
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
    let log_dir = cfg.storage.data_dir.join("log").join("sp6");
    let log = graydb_log::FrameLog::create(&log_dir, cfg.log.segment_max_bytes).await?;
    let durable_rx = log.durable();
    let metrics = Arc::new(IngestMetrics::default());
    let (ctrl_tx, ctrl_rx) = tokio::sync::watch::channel(PumpCommand::default());
    let pump = tokio::spawn(stream::run_pump(repl_b, log, lsn0, ctrl_rx, Arc::clone(&metrics)));

    let snapshot_dir = cfg.storage.data_dir.join("snapshot").join("sp6");
    if snapshot_dir.exists() {
        tokio::fs::remove_dir_all(&snapshot_dir).await.ok();
    }
    let manifest =
        snapshot::run_parallel_copy(&cfg, &slot.consistent_point, &slot.snapshot_name, &snapshot_dir)
            .await?;
    repl_a.close().await.ok();

    println!("[2/6] post-LSN0 workload with two capture points ...");
    admin
        .batch_execute(
            "INSERT INTO app.orders (customer_id, status, amount)
             SELECT 1 + (g % 5000), 'streamed', (g % 999)::numeric / 10
             FROM generate_series(1, 2000) g",
        )
        .await?;
    let l1 = current_lsn(&admin).await?;
    admin
        .execute("UPDATE app.orders SET status = 'reprocessed' WHERE id % 97 = 0", &[])
        .await?;
    admin
        .execute("UPDATE app.customers SET name = 'xylophone marmot' WHERE id = 42", &[])
        .await?;
    let l2 = current_lsn(&admin).await?;
    admin
        .execute("DELETE FROM app.orders WHERE id % 131 = 0 AND id % 97 <> 0", &[])
        .await?;
    let head_lsn = current_lsn(&admin).await?;

    println!("[3/6] custody wait, shutdown, replay ...");
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
    let received_lsn = durable_rx.borrow().lsn;
    ctrl_tx.send(PumpCommand { stalled: false, shutdown: true })?;
    pump.await.context("pump task")??;
    let replay = graydb_registry::replay_log(&log_dir)?;

    // ---- Materialize both shapes to disk -----------------------------------------
    println!("[4/6] materializing columnar (3 tables) + search (customers) ...");
    let col_root = cfg.storage.data_dir.join("columnar").join("sp6");
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
            TableStore::create(&col_root.join(&t.table), &t.table, specs, cfg.columnar.flush_rows)?;
        for part in &t.parts {
            let data = std::fs::read(snapshot_dir.join(&part.file))?;
            store.load_copy_part(&data, lsn0)?;
        }
        store.flush()?;
        stores.insert(t.table.clone(), store);
    }
    let search_root = cfg.storage.data_dir.join("search").join("sp6");
    let cust = manifest
        .tables
        .iter()
        .find(|t| t.table == "app.customers")
        .context("customers manifest")?;
    let mut search_store = SearchStore::create(
        &search_root.join("app.customers"),
        "app.customers",
        &["name".to_string(), "email".to_string()],
        &cust.key_columns,
    )?;
    let name_pos = cust.columns.iter().position(|c| c == "name").context("name")?;
    let email_pos = cust.columns.iter().position(|c| c == "email").context("email")?;
    let id_pos = cust.columns.iter().position(|c| c == "id").context("id")?;
    for part in &cust.parts {
        let data = std::fs::read(snapshot_dir.join(&part.file))?;
        for line in data.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
            let vals = copytext::parse_copy_line(line);
            let key = vals[id_pos].clone().unwrap_or_default();
            search_store.index_backfill_row(
                &key,
                &[vals[name_pos].as_deref(), vals[email_pos].as_deref()],
                lsn0,
            )?;
        }
    }
    search_store.commit_batch(lsn0)?;

    let mut prev_lsn = 0u64;
    for (idx, change) in replay.changes.iter().enumerate() {
        if let Some(store) = stores.get_mut(&change.table) {
            store.apply(change)?;
        } else if change.table != "graydb.ddl_log" {
            anyhow::bail!("change for unknown table {}", change.table);
        }
        if change.table == "app.customers" {
            search_store.apply(change, &format!("s:{idx}"))?;
        }
        prev_lsn = change.commit_lsn;
    }
    for store in stores.values_mut() {
        store.finalize()?;
    }
    search_store.commit_batch(prev_lsn)?;
    drop(search_store); // release the writer; the reader opens read-only

    // ---- The reader: SQL at target LSNs with proof --------------------------------
    println!("[5/6] reader queries at caller-supplied target LSNs ...\n");
    let reader = Reader::open(
        manifest
            .tables
            .iter()
            .map(|t| TableShape {
                name: t.table.clone(),
                dir: col_root.join(&t.table),
            })
            .collect(),
        vec![("app.customers".to_string(), search_root.join("app.customers"))],
        received_lsn,
    )?;

    let count_at = |lsn: Option<u64>| {
        let r = &reader;
        async move {
            let (batches, proof) = r
                .query("SELECT count(*) AS n FROM app.orders", lsn)
                .await?;
            Ok::<(i64, String), anyhow::Error>((first_i64(&batches, "n")?, proof.render()))
        }
    };
    let (n_lsn0, proof0) = count_at(Some(lsn0)).await?;
    let (n_l1, _) = count_at(Some(l1)).await?;
    let (n_head, proof_head) = count_at(None).await?;
    let src_now: i64 = admin.query_one("SELECT count(*) FROM app.orders", &[]).await?.get(0);
    println!("  count(orders) @LSN0={n_lsn0} @L1={n_l1} @head={n_head} (source now: {src_now})");
    println!("    {proof0}");
    println!("    {proof_head}");
    let counts_ok = n_lsn0 == 20000 && n_l1 == 22000 && n_head == src_now;

    let status_at = |lsn: u64| {
        let r = &reader;
        async move {
            let (batches, _) = r
                .query("SELECT status FROM app.orders WHERE id = 97", Some(lsn))
                .await?;
            Ok::<String, anyhow::Error>(first_str(&batches, "status")?)
        }
    };
    let s_l1 = status_at(l1).await?;
    let s_l2 = status_at(l2).await?;
    println!("  status(id=97) @L1={s_l1:?} @L2={s_l2:?}  (exact historical answers)");
    let history_ok = s_l1 != "reprocessed" && s_l2 == "reprocessed";

    let (batches, _) = reader
        .query(
            "SELECT c.name AS name, s.score AS score
             FROM search('app.customers', 'xylophone') s
             JOIN app.customers c ON CAST(c.id AS VARCHAR) = s.key",
            None,
        )
        .await?;
    let hit_name = first_str(&batches, "name")?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("  search('xylophone') JOIN customers -> {total_rows} row(s), name={hit_name:?}");
    let search_ok = total_rows == 1 && hit_name == "xylophone marmot";

    let (batches, _) = reader
        .query(
            "SELECT shape, received_lsn, applied_lsn, apply_lag_bytes
             FROM graydb.stat_replication ORDER BY shape",
            None,
        )
        .await?;
    println!("  graydb.stat_replication:");
    let mut stat_rows = 0;
    for b in &batches {
        let shape = col_str(b, 0)?;
        let recv = col_str(b, 1)?;
        let applied = col_str(b, 2)?;
        let lag = b
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("lag col")?;
        for i in 0..b.num_rows() {
            println!(
                "    {:<24} received={} applied={} lag={}B",
                shape.value(i), recv.value(i), applied.value(i), lag.value(i)
            );
            stat_rows += 1;
        }
    }
    let stat_ok = stat_rows == 4;

    println!("\n[6/6] verdict");
    println!("\n== SP6 verdict ==");
    println!("  target-LSN counts exact (LSN0/L1/head): {}", verdict(counts_ok));
    println!("  historical row answers exact (L1 vs L2): {}", verdict(history_ok));
    println!("  search() joins columnar               : {}", verdict(search_ok));
    println!("  graydb.stat_replication (4 shapes)    : {}", verdict(stat_ok));

    if counts_ok && history_ok && search_ok && stat_ok {
        println!("\nDEMO 5: PASS — every query names its LSN and gets exactly that database.");
        Ok(())
    } else {
        anyhow::bail!("DEMO 5: FAIL — see verdict table");
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

fn first_i64(batches: &[RecordBatch], col: &str) -> Result<i64> {
    for b in batches {
        if b.num_rows() > 0 {
            let idx = b.schema().index_of(col)?;
            let arr = b
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("not int64")?;
            return Ok(arr.value(0));
        }
    }
    anyhow::bail!("no rows for column {col}")
}

fn first_str(batches: &[RecordBatch], col: &str) -> Result<String> {
    for b in batches {
        if b.num_rows() > 0 {
            let idx = b.schema().index_of(col)?;
            let arr = b
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("not utf8")?;
            return Ok(arr.value(0).to_string());
        }
    }
    anyhow::bail!("no rows for column {col}")
}

fn col_str(b: &RecordBatch, idx: usize) -> Result<&StringArray> {
    b.column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .context("not utf8 column")
}
