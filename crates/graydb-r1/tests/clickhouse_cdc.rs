//! Service-backed ClickHouse CDC coverage (spec sections 10-12). Runs against
//! the r1 Compose stack; PostgreSQL provides the real WAL LSN clock. Run with
//! `cargo test -p graydb-r1 --test clickhouse_cdc -- --ignored --test-threads=1`.

use anyhow::{Context, Result};
use graydb_ingest::repl::{parse_lsn, ReplClient, ReplMsg};
use graydb_log::Frame;
use graydb_r1::clickhouse::{
    ApplyOutcome, ClickHouseAdapter, ClickHouseCdcAdapter, ClickHouseSink,
};
use graydb_r1::EngineAdapter;
use graydb_r1::{LogicalCheckpoint, QueryId, QueryInvocation, QueryParameters};
use graydb_registry::pgoutput::TupleValue;
use graydb_registry::{Op, TypedChange};
use sha2::{Digest, Sha256};
use std::sync::Mutex;
use std::time::Duration;
use tokio_postgres::NoTls;

const CLICKHOUSE_CDC_SLOT: &str = "graydb_r1_clickhouse_cdc_slot";

/// The two tests share the ClickHouse tables, so they serialize here.
static SERIAL: Mutex<()> = Mutex::new(());

fn clickhouse_url() -> String {
    std::env::var("CLICKHOUSE_HTTP").unwrap_or_else(|_| "http://127.0.0.1:58123".into())
}

fn operation_hash(label: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(label.as_bytes());
    format!("{:x}", hash.finalize())
}

async fn reset_clickhouse() {
    let sink = ClickHouseSink::new(clickhouse_url());
    sink.execute(
        "DROP TABLE IF EXISTS r1_tenants_raw; \
         DROP TABLE IF EXISTS r1_customers_raw; \
         DROP TABLE IF EXISTS r1_orders_raw; \
         DROP TABLE IF EXISTS r1_order_events_raw; \
         DROP DATABASE IF EXISTS r1_meta",
    )
    .await
    .unwrap();
    sink.execute(include_str!("../../../bench/r1/clickhouse.sql"))
        .await
        .unwrap();
}

async fn postgres_client() -> tokio_postgres::Client {
    let url = std::env::var("GRAYDB_R1_POSTGRES_URL")
        .expect("the r1 service environment supplies GRAYDB_R1_POSTGRES_URL");
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
        .execute("DROP TABLE IF EXISTS r1_ch_cdc_clock", &[])
        .await
        .unwrap();
    client
        .execute("CREATE TABLE r1_ch_cdc_clock (n bigint)", &[])
        .await
        .unwrap();
    client
}

fn replication_target(url: &str) -> Result<(String, u16, String, String, String)> {
    let stripped = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .context("R1 PostgreSQL URL must use postgres:// or postgresql://")?;
    let (credentials, address_and_database) = stripped
        .split_once('@')
        .context("R1 PostgreSQL URL must include user/password and host")?;
    let (user, password) = credentials
        .split_once(':')
        .context("R1 PostgreSQL URL must include a password")?;
    let (address, database) = address_and_database
        .split_once('/')
        .context("R1 PostgreSQL URL must include a database")?;
    let (host, port) = match address.rsplit_once(':') {
        Some((host, port)) => (
            host.to_owned(),
            port.parse().context("parsing PostgreSQL port")?,
        ),
        None => (address.to_owned(), 5432),
    };
    Ok((
        host,
        port,
        user.to_owned(),
        password.to_owned(),
        database.split('?').next().unwrap_or(database).to_owned(),
    ))
}

async fn application_repl(url: &str) -> Result<ReplClient> {
    let (host, port, user, password, database) = replication_target(url)?;
    ReplClient::connect(&host, port, &user, &password, &database).await
}

/// Commits a scratch transaction so the WAL advances, then reads the real
/// current LSN as the commit LSN for the next sink transaction.
async fn advance_lsn(client: &tokio_postgres::Client) -> u64 {
    client
        .execute("INSERT INTO r1_ch_cdc_clock VALUES (1)", &[])
        .await
        .unwrap();
    let lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    graydb_ingest::repl::parse_lsn(&lsn).unwrap()
}

fn order_change(op: Op, order_id: u64, status: &str, amount: i64) -> TypedChange {
    let new = (op == Op::Insert || op == Op::Update).then(|| {
        vec![
            ("order_id".into(), TupleValue::Text(order_id.to_string())),
            ("tenant_id".into(), TupleValue::Text("101".into())),
            ("customer_id".into(), TupleValue::Text("102".into())),
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
        xid: 7,
        table: "r1.orders".into(),
        op,
        new,
        old,
    }
}

/// Q5 (current order count and amount by status) evaluated exactly at `target_lsn`.
async fn q5_by_status(adapter: &ClickHouseAdapter, target_lsn: u64) -> Vec<(String, i64, i64)> {
    let result = adapter
        .query(&QueryInvocation {
            id: QueryId::Q5,
            parameters: QueryParameters {
                window_end_micros: 1_700_000_000_000_000,
                tenant_id: 1,
                tenant_set: vec![1],
            },
            checkpoint: LogicalCheckpoint {
                sequence: 1,
                source_lsn: target_lsn,
            },
            target_lsn,
        })
        .await
        .unwrap();
    let mut rows: Vec<(String, i64, i64)> = result
        .rows
        .iter()
        .map(|row| {
            (
                row[0].clone().unwrap(),
                row[1].clone().unwrap().parse().unwrap(),
                row[2].clone().unwrap().parse().unwrap(),
            )
        })
        .collect();
    rows.sort();
    rows
}

async fn one_cell(adapter: &ClickHouseAdapter, sql: &str) -> String {
    adapter
        .select(sql)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .into_iter()
        .next()
        .flatten()
        .unwrap()
}

#[tokio::test]
#[ignore = "requires the r1 ClickHouse service environment"]
async fn initial_load_and_roundtrip_are_exact_at_every_target_lsn() {
    let _guard = SERIAL.lock().unwrap();
    reset_clickhouse().await;
    let pg = postgres_client().await;
    let sink = ClickHouseSink::new(clickhouse_url());
    let adapter = ClickHouseAdapter::new(clickhouse_url());

    let lsn0 = advance_lsn(&pg).await;
    sink.apply_initial_load(
        lsn0,
        &[
            order_change(Op::Insert, 20_000_000_091, "pending", 300),
            order_change(Op::Insert, 20_000_000_092, "shipped", 400),
        ],
    )
    .await
    .unwrap();

    let lsn1 = advance_lsn(&pg).await;
    sink.apply_transaction(
        lsn1,
        &operation_hash("txn-1"),
        &[
            order_change(Op::Insert, 20_000_000_101, "paid", 100),
            order_change(Op::Insert, 20_000_000_102, "shipped", 200),
        ],
    )
    .await
    .unwrap();

    let lsn2 = advance_lsn(&pg).await;
    sink.apply_transaction(
        lsn2,
        &operation_hash("txn-2"),
        &[order_change(Op::Update, 20_000_000_101, "refunded", 150)],
    )
    .await
    .unwrap();

    let lsn3 = advance_lsn(&pg).await;
    sink.apply_transaction(
        lsn3,
        &operation_hash("txn-3"),
        &[order_change(Op::Delete, 20_000_000_102, "", 0)],
    )
    .await
    .unwrap();

    // Exact at each target LSN, evaluated AFTER every change is applied: a
    // stale-LSN query must not see the newer commit.
    let at_lsn0 = q5_by_status(&adapter, lsn0).await;
    assert_eq!(
        at_lsn0,
        vec![
            ("pending".to_string(), 1, 300),
            ("shipped".to_string(), 1, 400)
        ]
    );
    let at_lsn1 = q5_by_status(&adapter, lsn1).await;
    assert_eq!(
        at_lsn1,
        vec![
            ("paid".to_string(), 1, 100),
            ("pending".to_string(), 1, 300),
            ("shipped".to_string(), 1, 600),
        ]
    );
    let at_lsn2 = q5_by_status(&adapter, lsn2).await;
    assert_eq!(
        at_lsn2,
        vec![
            ("pending".to_string(), 1, 300),
            ("refunded".to_string(), 1, 150),
            ("shipped".to_string(), 1, 600),
        ]
    );
    let at_lsn3 = q5_by_status(&adapter, lsn3).await;
    assert_eq!(
        at_lsn3,
        vec![
            ("pending".to_string(), 1, 300),
            ("refunded".to_string(), 1, 150),
        ]
    );

    // The deleted order is a tombstone, not a live row, at its delete LSN.
    let visible = one_cell(
        &adapter,
        &format!(
            "SELECT count() FROM \
             (SELECT order_id, tupleElement(_row, 9) AS deleted \
              FROM (SELECT order_id, argMax((tenant_id, customer_id, status, channel, \
                    amount_cents, created_at, updated_at, attributes, _deleted), _version) AS _row \
                    FROM r1_orders_raw WHERE _source_lsn <= {lsn3} GROUP BY order_id) \
              WHERE deleted = 0)"
        ),
    )
    .await;
    assert_eq!(
        visible, "3",
        "two live orders plus the snapshot pair minus the delete"
    );

    // The raw tables deliberately retain every immutable version. Force the
    // strongest merge path and re-run a stale checkpoint after later updates
    // and deletes have been applied; the old target must still be reconstructible.
    sink.execute("OPTIMIZE TABLE r1_orders_raw FINAL")
        .await
        .unwrap();
    assert_eq!(
        q5_by_status(&adapter, lsn1).await,
        vec![
            ("paid".to_string(), 1, 100),
            ("pending".to_string(), 1, 300),
            ("shipped".to_string(), 1, 600),
        ]
    );
}

#[tokio::test]
#[ignore = "requires the Task 11 PostgreSQL logical-replication and ClickHouse service environment"]
async fn real_pgoutput_frames_are_buffered_until_commit_then_applied() -> Result<()> {
    let _guard = SERIAL.lock().unwrap();
    reset_clickhouse().await;
    let url = std::env::var("GRAYDB_R1_POSTGRES_URL")
        .expect("the r1 service environment supplies GRAYDB_R1_POSTGRES_URL");
    let pg = postgres_client().await;
    let order_id = 20_000_099_999_i64;
    // Clean up before the replication slot's snapshot so the stream contains
    // precisely the insert below, not a prior-run delete.
    pg.execute("DELETE FROM r1.orders WHERE order_id = $1", &[&order_id])
        .await?;
    pg.query(
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
         WHERE slot_name = $1 AND NOT active",
        &[&CLICKHOUSE_CDC_SLOT],
    )
    .await?;

    let mut repl = application_repl(&url).await?;
    let snapshot = repl.create_slot_with_snapshot(CLICKHOUSE_CDC_SLOT).await?;
    let initial_lsn = parse_lsn(&snapshot.consistent_point)?;
    repl.start_replication(CLICKHOUSE_CDC_SLOT, "graydb_r1_pub", initial_lsn)
        .await?;

    pg.execute(
        "INSERT INTO r1.orders (order_id, tenant_id, customer_id, status, channel, amount_cents, created_at, updated_at, attributes) \
         VALUES ($1, 101, 102, 'paid', 'web', 123, '2026-09-01 00:00:00+00', '2026-09-01 00:00:00+00', '{}'::jsonb)",
        &[&order_id],
    )
    .await?;

    let mut cdc = ClickHouseCdcAdapter::new(clickhouse_url());
    let mut seq = 0_u64;
    let applied = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match repl.next_replication_message().await? {
                ReplMsg::XLogData { wal_start, payload } => {
                    let frame = Frame {
                        seq,
                        lsn_start: wal_start,
                        lsn_end: wal_start,
                        txn_complete: false,
                        payload,
                    };
                    seq += 1;
                    if let Some(outcome) = cdc
                        .apply_frames(&mut repl, &operation_hash("real-pgoutput-order"), &[frame])
                        .await?
                    {
                        break Ok::<_, anyhow::Error>(outcome);
                    }
                }
                ReplMsg::Keepalive {
                    reply_requested: true,
                    ..
                } => repl.send_standby_status(initial_lsn, false).await?,
                ReplMsg::Keepalive { .. } => {}
            }
        }
    })
    .await
    .context("timed out waiting for real pgoutput Commit")??;
    assert_eq!(applied, ApplyOutcome::Applied);

    let adapter = ClickHouseAdapter::new(clickhouse_url());
    assert_eq!(
        one_cell(
            &adapter,
            &format!("SELECT count() FROM r1_orders_raw WHERE order_id = {order_id}"),
        )
        .await,
        "1"
    );
    repl.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the r1 ClickHouse service environment"]
async fn retry_and_restart_never_duplicate_an_applied_transaction() {
    let _guard = SERIAL.lock().unwrap();
    reset_clickhouse().await;
    let pg = postgres_client().await;
    let sink = ClickHouseSink::new(clickhouse_url());

    let lsn1 = advance_lsn(&pg).await;
    let changes1 = vec![order_change(Op::Insert, 20_000_000_201, "paid", 100)];
    assert_eq!(
        sink.apply_transaction(lsn1, &operation_hash("txn-a"), &changes1)
            .await
            .unwrap(),
        ApplyOutcome::Applied
    );
    // Retry the same transaction (transport failure replay): idempotent skip.
    assert_eq!(
        sink.apply_transaction(lsn1, &operation_hash("txn-a"), &changes1)
            .await
            .unwrap(),
        ApplyOutcome::SkippedIdempotent
    );
    // Restart: a fresh sink instance resumes and must not re-apply.
    let restarted = ClickHouseSink::new(clickhouse_url());
    assert_eq!(
        restarted
            .apply_transaction(lsn1, &operation_hash("txn-a"), &changes1)
            .await
            .unwrap(),
        ApplyOutcome::SkippedIdempotent
    );
    // Progression continues after the restart: the next transaction applies.
    let lsn2 = advance_lsn(&pg).await;
    assert_eq!(
        restarted
            .apply_transaction(
                lsn2,
                &operation_hash("txn-b"),
                &[order_change(Op::Insert, 20_000_000_202, "shipped", 200)]
            )
            .await
            .unwrap(),
        ApplyOutcome::Applied
    );

    let adapter = ClickHouseAdapter::new(clickhouse_url());
    let raw_rows = one_cell(&adapter, "SELECT count() FROM r1_orders_raw").await;
    assert_eq!(
        raw_rows, "2",
        "each change exists exactly once in the raw table"
    );
    let markers = one_cell(&adapter, "SELECT count() FROM r1_meta.applied_transactions").await;
    assert_eq!(markers, "2");
    let applied = one_cell(
        &adapter,
        "SELECT max(source_lsn) FROM r1_meta.applied_transactions",
    )
    .await;
    assert_eq!(applied, lsn2.to_string());
}
