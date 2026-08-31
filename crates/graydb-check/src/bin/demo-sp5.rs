//! SP5 demo: tantivy search indexes fed in commit-LSN batches, never mid-transaction.
//! Backfill (staged COPY parts) indexes at LSN0, the stream applies through the same
//! delete+re-add identity discipline, and graydb-check asserts:
//!   doc counts == source truth, updated docs searchable under NEW text only,
//!   deleted docs gone, batch commits land only at txn boundaries, and a full
//!   re-apply of the stream converges to the same index (crash-replay idempotency).
//! Run: `just demo-sp5` (pg17) or `just demo-sp5-pg16`.

use anyhow::{Context, Result};
use graydb_columnar::copytext;
use graydb_ingest::repl::{format_lsn, parse_lsn, ReplClient};
use graydb_ingest::stream::{IngestMetrics, PumpCommand};
use graydb_ingest::{attach, config::Config, snapshot, stream};
use graydb_search::SearchStore;
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
    println!("== GrayDB SP5 : tantivy search, commit-LSN batches ==");
    let admin = cfg.connect().await?;
    let version: String = admin.query_one("SHOW server_version", &[]).await?.get(0);
    println!(
        "source: {}:{}/{} ({version}); indexes: {}",
        cfg.source.host,
        cfg.source.port,
        cfg.source.dbname,
        cfg.search
            .indexes
            .iter()
            .map(|i| format!("{}({})", i.table, i.columns.join(",")))
            .collect::<Vec<_>>()
            .join(" ")
    );

    // ---- Fresh state, slot, pump, backfill --------------------------------------
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
    let log_dir = cfg.storage.data_dir.join("log").join("sp5");
    let log = graydb_log::FrameLog::create(&log_dir, cfg.log.segment_max_bytes).await?;
    let durable_rx = log.durable();
    let metrics = Arc::new(IngestMetrics::default());
    let (ctrl_tx, ctrl_rx) = tokio::sync::watch::channel(PumpCommand::default());
    let pump = tokio::spawn(stream::run_pump(repl_b, log, lsn0, ctrl_rx, Arc::clone(&metrics)));

    let snapshot_dir = cfg.storage.data_dir.join("snapshot").join("sp5");
    if snapshot_dir.exists() {
        tokio::fs::remove_dir_all(&snapshot_dir).await.ok();
    }
    let manifest =
        snapshot::run_parallel_copy(&cfg, &slot.consistent_point, &slot.snapshot_name, &snapshot_dir)
            .await?;
    repl_a.close().await.ok();

    // ---- Post-LSN0 workload: many small txns so batch commits are visible --------
    println!("[2/6] workload: 150 txns of inserts + update chain + delete ...");
    for g in 0..50i32 {
        admin
            .execute(
                "INSERT INTO app.customers (name, email, balance)
                 SELECT 'zephyr quokka ' || (g + $1 * 10), 'zqmail' || (g + $1 * 10) || '@x.com', 5
                 FROM generate_series(1, 10) g",
                &[&g],
            )
            .await?;
    }
    admin
        .execute("UPDATE app.customers SET name = 'tempname unicorna' WHERE id = 42", &[])
        .await?;
    admin
        .execute("UPDATE app.customers SET name = 'xylophone marmot' WHERE id = 42", &[])
        .await?;
    // Delete a STREAMED row (id > 5000: never referenced by app.orders FKs).
    admin
        .execute("DELETE FROM app.customers WHERE id = 5001", &[])
        .await?;
    for g in 0..100i32 {
        admin
            .execute(
                "INSERT INTO app.notes (body)
                 SELECT 'kumquat note ' || (g + $1 * 3) FROM generate_series(1, 3) g",
                &[&g],
            )
            .await?;
    }
    let head_lsn = current_lsn(&admin).await?;

    println!("[3/6] waiting for durable custody, then pump shutdown ...");
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

    // ---- Build search stores: backfill at LSN0, then commit-LSN-batched apply -----
    println!("[4/6] indexing backfill + applying stream in commit-LSN batches ...");
    let search_root = cfg.storage.data_dir.join("search").join("sp5");
    let mut stores: HashMap<String, SearchStore> = HashMap::new();
    for idx_cfg in &cfg.search.indexes {
        let t = manifest
            .tables
            .iter()
            .find(|t| t.table == idx_cfg.table)
            .with_context(|| format!("{} not in snapshot manifest", idx_cfg.table))?;
        let mut store = SearchStore::create(
            &search_root.join(&t.table),
            &t.table,
            &idx_cfg.columns,
            &t.key_columns,
        )?;
        // Backfill: project declared columns + key out of the staged COPY parts.
        let col_pos: Vec<usize> = idx_cfg
            .columns
            .iter()
            .map(|c| t.columns.iter().position(|x| x == c).context("indexed column missing"))
            .collect::<Result<_>>()?;
        let key_pos: Vec<usize> = t
            .key_columns
            .iter()
            .map(|c| t.columns.iter().position(|x| x == c).context("key column missing"))
            .collect::<Result<_>>()?;
        let mut indexed = 0u64;
        for part in &t.parts {
            let data = std::fs::read(snapshot_dir.join(&part.file))?;
            for (line_no, line) in data.split(|&b| b == b'\n').enumerate() {
                if line.is_empty() {
                    continue;
                }
                let vals = copytext::parse_copy_line(line);
                let key = if key_pos.is_empty() {
                    format!("b:{}:{}", part.file, line_no)
                } else {
                    key_pos
                        .iter()
                        .map(|&i| vals[i].clone().unwrap_or_default())
                        .collect::<Vec<_>>()
                        .join("\u{1f}")
                };
                let projected: Vec<Option<&str>> =
                    col_pos.iter().map(|&i| vals[i].as_deref()).collect();
                store.index_backfill_row(&key, &projected, lsn0)?;
                indexed += 1;
            }
        }
        store.commit_batch(lsn0)?;
        println!("  {:<16} backfill docs={indexed}", t.table);
        stores.insert(t.table.clone(), store);
    }

    let apply = |stores: &mut HashMap<String, SearchStore>| -> Result<(u64, u64)> {
        let batch = cfg.search.commit_batch_txns.max(1);
        let mut txns_completed = 0u64;
        let mut batch_commits = 0u64;
        let mut prev_lsn = 0u64;
        for (idx, change) in replay.changes.iter().enumerate() {
            if change.commit_lsn != prev_lsn {
                if prev_lsn != 0 {
                    txns_completed += 1;
                    if txns_completed % batch == 0 {
                        for s in stores.values_mut() {
                            s.commit_batch(prev_lsn)?; // boundary-only, never mid-txn
                        }
                        batch_commits += 1;
                    }
                }
                prev_lsn = change.commit_lsn;
            }
            if let Some(store) = stores.get_mut(&change.table) {
                store.apply(change, &format!("s:{idx}"))?;
            }
        }
        if prev_lsn != 0 {
            for s in stores.values_mut() {
                s.commit_batch(prev_lsn)?;
            }
            batch_commits += 1;
        }
        Ok((txns_completed + 1, batch_commits))
    };
    let started = std::time::Instant::now();
    let (txns, commits) = apply(&mut stores)?;
    println!(
        "  applied {} changes across {txns} txns in {:.2?}; {commits} batch commits (batch={} txns, boundary-only)",
        replay.changes.len(),
        started.elapsed(),
        cfg.search.commit_batch_txns
    );

    // ---- Checks ---------------------------------------------------------------------
    println!("[5/6] graydb-check: counts, update visibility, delete, batching, idempotency ...");
    let customers = stores.get("app.customers").context("customers index")?;
    let notes = stores.get("app.notes").context("notes index")?;

    let src_customers: i64 = admin.query_one("SELECT count(*) FROM app.customers", &[]).await?.get(0);
    let src_notes: i64 = admin.query_one("SELECT count(*) FROM app.notes", &[]).await?.get(0);
    let counts_ok = customers.num_docs()? == src_customers as u64
        && notes.num_docs()? == src_notes as u64;
    println!(
        "  docs: customers={} (source {src_customers})  notes={} (source {src_notes})",
        customers.num_docs()?,
        notes.num_docs()?
    );

    let zephyr = customers.search("zephyr", 2000)?.len();
    let xylo = customers.search("xylophone", 10)?;
    let unicorna = customers.search("unicorna", 10)?.len();
    let deleted_mail = customers.search("zqmail1", 10)?.len(); // id=5001's unique email token
    let survivor_mail = customers.search("zqmail2", 10)?.len();
    let kumquat = notes.search("kumquat", 5000)?.len();
    println!(
        "  search: zephyr={zephyr} (expect 499)  xylophone={:?}  unicorna={unicorna} (expect 0)  \
         zqmail1={deleted_mail} (expect 0)  zqmail2={survivor_mail} (expect 1)  kumquat={kumquat} (expect 300)",
        xylo.iter().map(|(k, _, _)| k.as_str()).collect::<Vec<_>>()
    );
    let update_ok = xylo.len() == 1 && xylo[0].0 == "42" && unicorna == 0;
    let delete_ok = deleted_mail == 0 && survivor_mail == 1;
    let insert_ok = zephyr == 499 && kumquat == 300;

    let watermark_ok = customers.meta.applied_lsn == notes.meta.applied_lsn
        && customers.meta.applied_lsn > lsn0;
    let batching_ok = commits >= 2 && txns >= cfg.search.commit_batch_txns;
    println!(
        "  applied_lsn={}  txns={txns} batch_commits={commits}",
        format_lsn(customers.meta.applied_lsn)
    );

    // Idempotency at demo level: re-apply the ENTIRE stream; index must not change.
    let (docs_before_c, docs_before_n) = (customers.num_docs()?, notes.num_docs()?);
    let _ = apply(&mut stores)?;
    let customers = stores.get("app.customers").unwrap();
    let notes = stores.get("app.notes").unwrap();
    let idempotent_ok = customers.num_docs()? == docs_before_c
        && notes.num_docs()? == docs_before_n
        && customers.search("zephyr", 2000)?.len() == 499;
    println!(
        "  full re-apply: customers={} notes={} (unchanged: {idempotent_ok})",
        customers.num_docs()?,
        notes.num_docs()?
    );

    println!("\n[6/6] verdict");
    println!("\n== SP5 verdict ==");
    println!("  doc counts == source              : {}", verdict(counts_ok));
    println!("  inserts searchable                : {}", verdict(insert_ok));
    println!("  update = delete+re-add (BM25)     : {}", verdict(update_ok));
    println!("  delete drops the document         : {}", verdict(delete_ok));
    println!("  commit-LSN batching, boundary-only: {}", verdict(batching_ok));
    println!("  applied_lsn watermark coherent    : {}", verdict(watermark_ok));
    println!("  full re-apply idempotent          : {}", verdict(idempotent_ok));

    if counts_ok && insert_ok && update_ok && delete_ok && batching_ok && watermark_ok && idempotent_ok {
        println!("\nSP5: PASS — search follows the log in commit order; freshness is a watermark, not a guess.");
        Ok(())
    } else {
        anyhow::bail!("SP5: FAIL — see verdict table");
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
