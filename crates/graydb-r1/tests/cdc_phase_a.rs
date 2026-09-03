//! Task 14 Phase A: reproduce and measure the ClickHouse CDC apply budget
//! against the live Compose stack. Run with
//! `cargo test -p graydb-r1 --test cdc_phase_a -- --ignored --test-threads=1`.
//!
//! Measures: applied transactions/second, HTTP requests per transaction,
//! insert/marker/verification time, and the partial-failure retry boundary
//! (rows inserted, marker missing, then full replay) with deterministic
//! deduplication tokens. Also verifies the source writer sustains the frozen
//! 300 rows/second target.

use anyhow::{Context, Result};
use graydb_r1::clickhouse::ClickHouseSink;
use graydb_r1::{
    ApplyOutcome, EngineKind, IntentLog, ProfileCatalog, R1RuntimeServices, RunMode, RunPlan,
    ScaleProfile, SinkMetricsSnapshot, SystemR1RuntimeServices,
};
use graydb_registry::pgoutput::TupleValue;
use graydb_registry::{Op, TypedChange};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio_postgres::NoTls;

static SERIAL: Mutex<()> = Mutex::new(());

fn clickhouse_url() -> String {
    std::env::var("CLICKHOUSE_HTTP").unwrap_or_else(|_| "http://127.0.0.1:58123".into())
}

fn postgres_url() -> String {
    std::env::var("GRAYDB_R1_POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://r1:graydb_r1@127.0.0.1:55432/r1".into())
}

fn operation_hash(label: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(label.as_bytes());
    format!("{:x}", hash.finalize())
}

async fn pg_client() -> Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(&postgres_url(), NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

async fn reset_clickhouse() -> Result<ClickHouseSink> {
    let sink = ClickHouseSink::new(clickhouse_url());
    sink.execute(
        "DROP TABLE IF EXISTS r1_tenants_raw; \
         DROP TABLE IF EXISTS r1_customers_raw; \
         DROP TABLE IF EXISTS r1_orders_raw; \
         DROP TABLE IF EXISTS r1_order_events_raw; \
         DROP DATABASE IF EXISTS r1_meta",
    )
    .await?;
    sink.execute(include_str!("../../../bench/r1/clickhouse.sql"))
        .await?;
    Ok(sink)
}

async fn advance_lsn(client: &tokio_postgres::Client) -> Result<u64> {
    client
        .execute("INSERT INTO r1_phase_a_clock VALUES (1)", &[])
        .await?;
    let lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    Ok(graydb_ingest::repl::parse_lsn(&lsn)?)
}

fn order_change(op: Op, order_id: u64, status: &str, amount: i64) -> TypedChange {
    let new = (op != Op::Delete).then(|| {
        vec![
            ("order_id".into(), TupleValue::Text(order_id.to_string())),
            ("tenant_id".into(), TupleValue::Text("701".into())),
            ("customer_id".into(), TupleValue::Text("702".into())),
            ("status".into(), TupleValue::Text(status.into())),
            ("channel".into(), TupleValue::Text("web".into())),
            ("amount_cents".into(), TupleValue::Text(amount.to_string())),
            (
                "created_at".into(),
                TupleValue::Text("2026-09-01 00:00:00".into()),
            ),
            (
                "updated_at".into(),
                TupleValue::Text("2026-09-01 00:00:01".into()),
            ),
            ("attributes".into(), TupleValue::Text("{\"k\":1}".into())),
        ]
    });
    let old = (op == Op::Delete)
        .then(|| vec![("order_id".into(), TupleValue::Text(order_id.to_string()))]);
    TypedChange {
        commit_lsn: 0,
        xid: 9100,
        table: "r1.orders".into(),
        op,
        new,
        old,
    }
}

fn event_change(event_id: u64, order_id: u64) -> TypedChange {
    TypedChange {
        commit_lsn: 0,
        xid: 9100,
        table: "r1.order_events".into(),
        op: Op::Insert,
        new: Some(vec![
            ("event_id".into(), TupleValue::Text(event_id.to_string())),
            ("order_id".into(), TupleValue::Text(order_id.to_string())),
            ("tenant_id".into(), TupleValue::Text("701".into())),
            ("event_type".into(), TupleValue::Text("created".into())),
            (
                "event_at".into(),
                TupleValue::Text("2026-09-01 00:00:02".into()),
            ),
            (
                "metadata".into(),
                TupleValue::Text("{\"phase\":\"a\"}".into()),
            ),
        ]),
        old: None,
    }
}

/// One deterministic Phase A transaction: one order insert, one event insert,
/// and periodically an older-order update or delete — mirroring the workload
/// mix's shape at small scale.
fn phase_a_changes(lsn: u64, ordinal: u64) -> Vec<TypedChange> {
    let order_id = 40_000_000_000 + ordinal;
    let event_id = 40_000_000_000 + ordinal;
    let mut changes = vec![
        order_change(
            Op::Insert,
            order_id,
            "pending",
            100 + (ordinal % 900) as i64,
        ),
        event_change(event_id, order_id),
    ];
    if ordinal > 4 && ordinal % 4 == 0 {
        changes.push(order_change(
            Op::Update,
            order_id - 4,
            "shipped",
            100 + ((ordinal - 4) % 900) as i64,
        ));
    }
    if ordinal > 10 && ordinal % 10 == 0 {
        changes.push(order_change(Op::Delete, order_id - 10, "", 0));
    }
    for change in &mut changes {
        change.commit_lsn = lsn;
    }
    changes
}

async fn count_cell(sink: &ClickHouseSink, sql: &str) -> Result<u64> {
    let (body, _) = sink
        .post(&[], &format!("{sql} FORMAT TabSeparated"))
        .await?;
    Ok(body.trim().parse().context("parsing count")?)
}

async fn count_pair(sink: &ClickHouseSink, sql: &str) -> Result<(u64, u64)> {
    let (body, _) = sink
        .post(&[], &format!("{sql} FORMAT TabSeparated"))
        .await?;
    let (left, right) = body
        .trim()
        .split_once('\t')
        .context("expected two tab-separated counts")?;
    Ok((
        left.parse().context("parsing count")?,
        right.parse().context("parsing uniqExact")?,
    ))
}

#[tokio::test]
#[ignore = "requires the live ClickHouse and PostgreSQL services"]
async fn phase_a_measures_throughput_and_proves_token_retry_dedup() -> Result<()> {
    let _guard = SERIAL.lock().unwrap();
    let sink = reset_clickhouse().await?;
    let pg = pg_client().await?;
    pg.execute("DROP TABLE IF EXISTS r1_phase_a_clock", &[])
        .await?;
    pg.execute("CREATE TABLE r1_phase_a_clock (n bigint)", &[])
        .await?;

    const TRANSACTIONS: u64 = 2_000;
    let started = Instant::now();
    for ordinal in 1..=TRANSACTIONS {
        let lsn = advance_lsn(&pg).await?;
        let hash = operation_hash(&format!("phase-a-{ordinal}"));
        let changes = phase_a_changes(lsn, ordinal);
        sink.apply_transaction(lsn, &hash, &changes).await?;
    }
    let elapsed = started.elapsed();

    let metrics: SinkMetricsSnapshot = sink.metrics();
    let applied_per_second = TRANSACTIONS as f64 / elapsed.as_secs_f64();
    println!("== Phase A throughput ({TRANSACTIONS} transactions) ==");
    println!(
        "applied transactions/second: {:.1} (elapsed {:.1}s)",
        applied_per_second,
        elapsed.as_secs_f64()
    );
    println!(
        "HTTP requests per transaction: {:.2}",
        metrics.requests as f64 / TRANSACTIONS as f64
    );
    println!(
        "insert {:.2} ms/txn | marker {:.2} ms/txn | verify {:.2} ms/txn | total HTTP {:.2} ms/txn",
        metrics.insert_ns as f64 / 1e6 / TRANSACTIONS as f64,
        metrics.marker_ns as f64 / 1e6 / TRANSACTIONS as f64,
        metrics.verify_ns as f64 / 1e6 / TRANSACTIONS as f64,
        metrics.request_ns as f64 / 1e6 / TRANSACTIONS as f64
    );

    // Row integrity after a clean sequential apply.  Orders is a VERSIONED
    // table (updates and deletes append rows), so only the distinct count is
    // invariant there; events are insert-only, so count == uniqExact holds.
    let (total, distinct) = count_pair(
        &sink,
        &format!(
            "SELECT count(), uniqExact(order_id) FROM r1_orders_raw WHERE order_id >= 40_000_000_000"
        ),
    )
    .await?;
    assert_eq!(
        distinct, TRANSACTIONS,
        "orders distinct keys after clean apply"
    );
    println!("orders versions after clean apply: {total} rows for {distinct} keys");
    let (total, distinct) = count_pair(
        &sink,
        &format!(
            "SELECT count(), uniqExact(event_id) FROM r1_order_events_raw WHERE event_id >= 40_000_000_000"
        ),
    )
    .await?;
    assert_eq!(
        (total, distinct),
        (TRANSACTIONS, TRANSACTIONS),
        "events must be insert-only with no duplicate keys"
    );

    // Partial-failure boundary: rows land, marker does not.
    let partial_ordinal = TRANSACTIONS + 1;
    let partial_lsn = advance_lsn(&pg).await?;
    let partial_hash = operation_hash(&format!("phase-a-{partial_ordinal}"));
    let partial_changes = phase_a_changes(partial_lsn, partial_ordinal);
    sink.apply_rows_without_marker(partial_lsn, &partial_hash, &partial_changes)
        .await?;
    let markers = count_cell(
        &sink,
        &format!(
            "SELECT count() FROM r1_meta.applied_transactions WHERE source_lsn = {partial_lsn}"
        ),
    )
    .await?;
    assert_eq!(markers, 0, "partial apply must not write a marker");
    println!("partial failure injected at LSN {partial_lsn:#x}: rows present, marker absent");

    // Replay from the last acknowledged LSN re-applies the whole transaction.
    let replay = sink
        .apply_transaction(partial_lsn, &partial_hash, &partial_changes)
        .await?;
    let retry_tokens = [
        "r1_tenants_raw",
        "r1_customers_raw",
        "r1_orders_raw",
        "r1_order_events_raw",
    ]
    .map(|table| format!("{partial_hash}:{table}"))
    .join(", ");
    println!("replay outcome: {replay:?}; retry tokens: {retry_tokens}");

    // After replay: events are insert-only, so count == uniqExact must hold
    // with exactly one more row (the partial transaction's event).
    let (total, distinct) = count_pair(
        &sink,
        &format!(
            "SELECT count(), uniqExact(event_id) FROM r1_order_events_raw WHERE event_id >= 40_000_000_000"
        ),
    )
    .await?;
    assert_eq!(
        (total, distinct),
        (TRANSACTIONS + 1, TRANSACTIONS + 1),
        "replay after partial failure created {total} event rows for {distinct} distinct ids"
    );
    let (total, distinct) = count_pair(
        &sink,
        &format!(
            "SELECT count(), uniqExact(order_id) FROM r1_orders_raw WHERE order_id >= 40_000_000_000"
        ),
    )
    .await?;
    assert_eq!(
        distinct,
        TRANSACTIONS + 1,
        "replay must not create a second order identity"
    );
    println!("orders versions after replay: {total} rows for {distinct} keys");
    let markers = count_cell(
        &sink,
        &format!(
            "SELECT count() FROM r1_meta.applied_transactions WHERE source_lsn = {partial_lsn}"
        ),
    )
    .await?;
    assert_eq!(markers, 1, "exactly one applied marker per source LSN");

    // Idempotent re-apply is a skip, never a second write.
    let repeat = sink
        .apply_transaction(partial_lsn, &partial_hash, &partial_changes)
        .await?;
    assert_eq!(repeat, ApplyOutcome::SkippedIdempotent);
    println!("Phase A: token retry dedup PROVEN (no duplicate keys after replay)");
    Ok(())
}

#[tokio::test]
#[ignore = "requires the live PostgreSQL service and control publications"]
async fn phase_a_source_writer_sustains_300_rows_per_second() -> Result<()> {
    let _guard = SERIAL.lock().unwrap();
    let pg = pg_client().await?;
    pg.execute(
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active",
        &[],
    )
    .await?;
    pg.execute(
        "TRUNCATE r1.order_events, r1.orders, r1.customers, r1.tenants, r1_control.tx_marker",
        &[],
    )
    .await?;

    let run_dir = tempfile::tempdir()?;
    let catalog = ProfileCatalog::load(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/r1/profiles.toml"),
    )?;
    let plan = RunPlan {
        profile: ScaleProfile::MacSmoke,
        spec: catalog.get(ScaleProfile::MacSmoke).unwrap().clone(),
        mode: RunMode::Correctness,
        engines: vec![EngineKind::Graydb, EngineKind::Clickhouse],
        input_hashes: BTreeMap::new(),
    };
    let mut services = SystemR1RuntimeServices::from_env()?;
    services.bind_run(run_dir.path(), &plan)?;

    services.set_writer_rate(Some(300)).await?;
    let window = Duration::from_secs(20);
    let started = Instant::now();
    tokio::time::sleep(window).await;
    services.set_writer_rate(None).await?;
    let elapsed = started.elapsed().as_secs_f64();

    // Affected rows from the durable intent log: every planned operation the
    // writer committed during the window.
    let intents = IntentLog::create(run_dir.path())?.read_all()?;
    let affected_rows: u64 = intents
        .iter()
        .map(|plan| plan.operations.len() as u64)
        .sum();
    let rows_per_second = affected_rows as f64 / elapsed;
    println!(
        "== Phase A source writer == intents {} affected rows {} in {:.1}s => {:.1} rows/s (target 300)",
        intents.len(),
        affected_rows,
        elapsed,
        rows_per_second
    );
    assert!(
        rows_per_second >= 0.95 * 300.0,
        "source writer reached {rows_per_second:.1} rows/s; the frozen 300 rows/s target requires at least 285"
    );
    Ok(())
}
