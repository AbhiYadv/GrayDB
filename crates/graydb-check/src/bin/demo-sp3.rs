//! Demo 6 (SP3): one additive DDL (ADD COLUMN) + one destructive DDL (DROP COLUMN)
//! flow through IN-STREAM with correct per-LSN interpretation.
//! Three eras of inserts into app.customers around the two ALTERs; everything is then
//! replayed FROM THE DURABLE FRAME LOG ALONE (I2/I3): the registry must show three
//! schema versions at the right commit-LSN boundaries, every insert must decode under
//! the schema in force at its position, the ddl_log rows must sit strictly between
//! the eras, and the replay must be deterministic (same frames -> same result).
//! Run: `just demo-sp3` (pg17) or `just demo-sp3-pg16`.

use anyhow::{Context, Result};
use graydb_ingest::repl::{format_lsn, parse_lsn, ReplClient};
use graydb_ingest::stream::{IngestMetrics, PumpCommand};
use graydb_ingest::{attach, config::Config, stream};
use graydb_registry::pgoutput::TupleValue;
use graydb_registry::{replay_log, Op};
use std::sync::Arc;
use std::time::Duration;

const SEED_SQL: &str = include_str!("../../../../db/seed.sql");
const ERA_ROWS: i64 = 100;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::load()?;
    println!("== GrayDB SP3 / Demo 6 : DDL in-stream with per-LSN interpretation ==");
    let admin = cfg.connect().await?;
    let version: String = admin.query_one("SHOW server_version", &[]).await?.get(0);
    println!(
        "source: {}:{}/{} ({version})",
        cfg.source.host, cfg.source.port, cfg.source.dbname
    );

    // ---- Fresh state + stream from LSN0 -----------------------------------------
    attach::drop_slot_if_exists(&admin, &cfg.source.slot).await?;
    attach::drop_publication_if_exists(&admin, &cfg.source.publication).await?;
    println!("\n[1/5] seed + attach + slot + pump ...");
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
    repl_a.close().await.ok(); // no snapshot load in this demo; stream is the subject
    println!("  LSN0={}", slot.consistent_point);

    let mut repl_b = ReplClient::connect(
        &cfg.source.host, cfg.source.port, &cfg.source.user,
        &cfg.source.password, &cfg.source.dbname,
    ).await?;
    repl_b
        .start_replication(&cfg.source.slot, &cfg.source.publication, lsn0)
        .await?;
    let log_dir = cfg.storage.data_dir.join("log").join("sp3");
    let log = graydb_log::FrameLog::create(&log_dir, cfg.log.segment_max_bytes).await?;
    let durable_rx = log.durable();
    let metrics = Arc::new(IngestMetrics::default());
    let (ctrl_tx, ctrl_rx) = tokio::sync::watch::channel(PumpCommand::default());
    let pump = tokio::spawn(stream::run_pump(repl_b, log, lsn0, ctrl_rx, Arc::clone(&metrics)));

    // ---- Three eras around two DDLs ----------------------------------------------
    println!("[2/5] era 1 inserts -> ADD COLUMN -> era 2 -> DROP COLUMN -> era 3 ...");
    let insert_era = |era: i64| {
        format!(
            "INSERT INTO app.customers (name, email, balance)
             SELECT 'era{era}_' || g, 'era{era}_' || g || '@x.com', {era}
             FROM generate_series(1, {ERA_ROWS}) g"
        )
    };
    admin.batch_execute(&insert_era(1)).await?;
    let lsn_after_era1 = current_lsn(&admin).await?;

    // NOTE: the registry's version boundary is the commit of the txn carrying the
    // NEW Relation message — i.e. the schema's first in-stream USE, not the ALTER
    // itself. The ALTER's own position is captured by the ddl_log event; both are
    // asserted below. So we deliberately do not sample schema_for between the ALTER
    // and the next insert.
    admin
        .batch_execute("ALTER TABLE app.customers ADD COLUMN city text DEFAULT 'unknown'")
        .await?;

    admin
        .batch_execute(
            "INSERT INTO app.customers (name, email, balance, city)
             SELECT 'era2_' || g, 'era2_' || g || '@x.com', 2, 'city_' || (g % 7)
             FROM generate_series(1, 100) g",
        )
        .await?;
    let lsn_after_era2 = current_lsn(&admin).await?;

    admin
        .batch_execute("ALTER TABLE app.customers DROP COLUMN city")
        .await?;

    admin.batch_execute(&insert_era(3)).await?;
    let lsn_after_era3 = current_lsn(&admin).await?;

    // ---- Wait for full durable custody, then stop the pump -----------------------
    println!("[3/5] waiting for durable custody of everything, then pump shutdown ...");
    let mut caught_up = false;
    for _ in 0..120 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let m = *durable_rx.borrow();
        if m.valid && m.lsn >= lsn_after_era3 {
            caught_up = true;
            break;
        }
    }
    anyhow::ensure!(caught_up, "pump did not reach {}", format_lsn(lsn_after_era3));
    ctrl_tx.send(PumpCommand { stalled: false, shutdown: true })?;
    pump.await.context("pump task")??;

    // ---- Replay from frames alone -------------------------------------------------
    println!("[4/5] replaying the frame log -> typed changes + registry + in-stream DDL ...");
    let replay = replay_log(&log_dir)?;
    let replay2 = replay_log(&log_dir)?; // determinism witness
    let deterministic = serde_json::to_vec(&replay.registry)? == serde_json::to_vec(&replay2.registry)?
        && replay.changes == replay2.changes
        && replay.ddl_events == replay2.ddl_events;

    let registry_path = cfg.storage.data_dir.join("registry").join("sp3.json");
    replay.registry.persist(&registry_path)?;

    // Registry: exactly the customer table's version history we caused.
    let customers = replay
        .registry
        .tables
        .values()
        .find(|t| t.qualified_name == "app.customers")
        .context("app.customers not in registry")?;
    println!("  registry versions for app.customers:");
    for v in &customers.versions {
        println!(
            "    from {}  cols={} [{}]",
            format_lsn(v.valid_from_lsn),
            v.columns.len(),
            v.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join(",")
        );
    }
    let shapes: Vec<(usize, bool)> = customers
        .versions
        .iter()
        .map(|v| (v.columns.len(), v.columns.iter().any(|c| c.name == "city")))
        .collect();
    let versions_ok = shapes == vec![(6, false), (7, true), (6, false)];

    // Typed inserts per era decode under the era's schema.
    let era_inserts = |needle: &str| -> Vec<&graydb_registry::TypedChange> {
        replay
            .changes
            .iter()
            .filter(|c| {
                c.table == "app.customers"
                    && c.op == Op::Insert
                    && c.new.as_ref().is_some_and(|n| {
                        n.iter().any(|(k, v)| {
                            k == "name" && matches!(v, TupleValue::Text(s) if s.starts_with(needle))
                        })
                    })
            })
            .collect()
    };
    let (e1, e2, e3) = (era_inserts("era1_"), era_inserts("era2_"), era_inserts("era3_"));
    let has_city = |c: &graydb_registry::TypedChange| {
        c.new.as_ref().is_some_and(|n| n.iter().any(|(k, _)| k == "city"))
    };
    let era1_ok = e1.len() == ERA_ROWS as usize && e1.iter().all(|c| !has_city(c));
    let era2_ok = e2.len() == ERA_ROWS as usize && e2.iter().all(|c| has_city(c));
    let era3_ok = e3.len() == ERA_ROWS as usize && e3.iter().all(|c| !has_city(c));

    // Per-LSN interpretation via the registry, sampled inside each era.
    let sample = |lsn: u64| {
        replay
            .registry
            .schema_for_table("app.customers", lsn)
            .map(|v| (v.columns.len(), v.columns.iter().any(|c| c.name == "city")))
    };
    let lsn_queries_ok = sample(lsn_after_era1) == Some((6, false))
        && sample(lsn_after_era2) == Some((7, true))
        && sample(lsn_after_era3) == Some((6, false));

    // In-stream DDL ordering: ALTERs sit strictly between the eras.
    // Assert on ddl_command_end rows only: sql_drop additionally fires per dropped
    // object (the column AND its default), which is correct capture but would
    // triple-count the DROP here.
    let alters: Vec<&graydb_registry::DdlEvent> = replay
        .ddl_events
        .iter()
        .filter(|d| d.kind == "command" && d.command_tag.as_deref() == Some("ALTER TABLE"))
        .collect();
    println!("  in-stream DDL events:");
    for d in &alters {
        println!(
            "    @{}  {}  {}",
            format_lsn(d.commit_lsn),
            d.object_identity.as_deref().unwrap_or("?"),
            d.ddl_text.as_deref().unwrap_or("?")
        );
    }
    let max_commit = |changes: &[&graydb_registry::TypedChange]| {
        changes.iter().map(|c| c.commit_lsn).max().unwrap_or(0)
    };
    let min_commit = |changes: &[&graydb_registry::TypedChange]| {
        changes.iter().map(|c| c.commit_lsn).min().unwrap_or(u64::MAX)
    };
    let ddl_ordering_ok = alters.len() == 2
        && alters[0].commit_lsn > max_commit(&e1)
        && alters[0].commit_lsn < min_commit(&e2)
        && alters[1].commit_lsn > max_commit(&e2)
        && alters[1].commit_lsn < min_commit(&e3)
        && alters[0].ddl_text.as_deref().is_some_and(|s| s.contains("ADD COLUMN"))
        && alters[1].ddl_text.as_deref().is_some_and(|s| s.contains("DROP COLUMN"));

    // ---- Verdict -------------------------------------------------------------------
    println!(
        "\n[5/5] replay: {} frames, {} txns, {} typed changes, {} ddl events (registry -> {})",
        replay.frames, replay.txns, replay.changes.len(), replay.ddl_events.len(),
        registry_path.display()
    );
    println!("\n== SP3 verdict ==");
    println!("  registry versions 6 -> 7(+city) -> 6 : {}", verdict(versions_ok));
    println!("  era decode under era schema          : {}", verdict(era1_ok && era2_ok && era3_ok));
    println!("  schema_for_table at sampled LSNs     : {}", verdict(lsn_queries_ok));
    println!("  DDL strictly between eras (in-stream): {}", verdict(ddl_ordering_ok));
    println!("  replay deterministic                 : {}", verdict(deterministic));

    if versions_ok && era1_ok && era2_ok && era3_ok && lsn_queries_ok && ddl_ordering_ok && deterministic {
        println!("\nDEMO 6: PASS — additive + destructive DDL flow through with correct per-LSN interpretation.");
        Ok(())
    } else {
        anyhow::bail!("DEMO 6: FAIL — see verdict table");
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
