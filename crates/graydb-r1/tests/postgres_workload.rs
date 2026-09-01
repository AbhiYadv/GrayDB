//! Service-backed control-stream and writer recovery coverage. Task 11 supplies
//! the PostgreSQL logical-replication service and runs this ignored test.

use anyhow::{anyhow, Context, Result};
use graydb_r1::replication::{
    run_control_replication_with_ready, ApplicationWriter, ControlReplicationConfig,
    PostgresCommitRecovery, ReplayMap, WorkloadReplayer, CONTROL_SLOT,
};
use graydb_r1::{CommittedLedger, IntentLog, WorkloadPlanner};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_postgres::{Client, NoTls};

const SEED: u64 = 20_260_901;

async fn connect(url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

#[tokio::test]
#[ignore = "requires the Task 11 PostgreSQL logical-replication service environment"]
async fn commits_mixed_workload_recovers_unknown_commit_and_preserves_ledger() -> Result<()> {
    let url = std::env::var("R1_POSTGRES_URL")
        .expect("Task 11 must set R1_POSTGRES_URL for the ignored service test");
    let admin = connect(&url).await?;
    // A re-run is allowed only after releasing the previous test slot. The schema
    // and publications are created by bench/r1/schema.sql / postgres_dataset.
    admin
        .query(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&CONTROL_SLOT],
        )
        .await?;
    admin
        .batch_execute(
            "
            DELETE FROM r1_control.tx_marker WHERE sequence BETWEEN 1 AND 100;
            DELETE FROM r1.order_events WHERE event_id >= 30000000001 AND event_id < 30000001000;
            DELETE FROM r1.orders WHERE order_id >= 20000000001 AND order_id < 20000001000;
            DELETE FROM r1.customers WHERE customer_id >= 10000000001 AND customer_id < 10000001000;
            ",
        )
        .await?;

    let run_dir = tempfile::tempdir()?;
    let (mapped_tx, mapped_rx) = mpsc::channel(256);
    let (stop_tx, stop_rx) = watch::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let control = tokio::spawn(run_control_replication_with_ready(
        replication_config(&url, run_dir.path().join("control-frame-log"))?,
        mapped_tx,
        stop_rx,
        Some(ready_tx),
    ));
    ready_rx
        .await
        .context("control replication task ended before readiness")?
        .map_err(|error| anyhow!("control replication setup failed: {error}"))?;

    let plans: Vec<_> = (1..=100)
        .map(|sequence| WorkloadPlanner::new(SEED).plan(sequence))
        .collect();
    let recovery = Arc::new(PostgresCommitRecovery::new(url.clone()));
    let mut writer = ApplicationWriter::new(
        connect(&url).await?,
        recovery.clone(),
        WorkloadPlanner::new(SEED),
        IntentLog::create(run_dir.path())?,
        CommittedLedger::create(run_dir.path())?,
        mapped_rx,
    );

    // The first 56 transactions exercise application SQL, control frames, mapper
    // delivery, and ledger append as one pipeline.
    for plan in &plans[..56] {
        writer.execute_and_record(plan).await?;
    }

    // Submit #57 then terminate the writer before its mapper message is consumed
    // or a ledger entry is appended. The receiver survives the process boundary.
    writer.submit_plan(&plans[56]).await?;
    let mapped_rx = writer.into_mapped_receiver();
    let mut resumed = ApplicationWriter::new(
        connect(&url).await?,
        recovery,
        WorkloadPlanner::new(SEED),
        IntentLog::create(run_dir.path())?,
        CommittedLedger::resume(run_dir.path())?,
        mapped_rx,
    );
    resumed.recover_and_record(&plans[56]).await?;
    for plan in &plans[57..] {
        resumed.execute_and_record(plan).await?;
    }

    let unique_markers: i64 = admin
        .query_one(
            "SELECT count(DISTINCT sequence) FROM r1_control.tx_marker WHERE sequence BETWEEN 1 AND 100",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(unique_markers, 100);
    assert_eq!(resumed.ledger().entries().len(), 100);
    assert_eq!(resumed.ledger().next_sequence(), 101);

    // Replay validation consumes the committed plans and records a distinct
    // sequence-to-LSN map. A restored source assigns different numeric LSNs;
    // logical sequence and operation hash must remain identical.
    let replay_entries = plans
        .iter()
        .cloned()
        .zip(resumed.ledger().entries().iter().cloned())
        .map(|(plan, committed)| {
            let replay_lsn = committed.source_lsn + 0x10_000;
            (plan, committed, replay_lsn)
        })
        .collect::<Vec<_>>();
    let mut replayer = WorkloadReplayer::new(ReplayMap::create(run_dir.path().join("replay"))?);
    replayer.replay(&replay_entries)?;
    assert_eq!(replayer.replay_map().entries().len(), 100);

    // A fresh recovery connection observes the same logical application rows as
    // the resumed writer; this detects a post-crash lost or duplicated write.
    let writer_digest = application_row_digest(&admin).await?;
    let recovery_digest = application_row_digest(&connect(&url).await?).await?;
    assert_eq!(writer_digest, recovery_digest);

    let _ = stop_tx.send(true);
    let joined = tokio::time::timeout(std::time::Duration::from_secs(10), control)
        .await
        .context("control replication did not stop")?;
    let control_result = joined.context("control replication task panicked")?;
    control_result.context("control replication failed")?;
    Ok(())
}

fn replication_config(
    url: &str,
    frame_log_dir: impl AsRef<Path>,
) -> Result<ControlReplicationConfig> {
    let stripped = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .context("R1_POSTGRES_URL must be a postgres URL")?;
    let (credentials, address_and_database) = stripped
        .split_once('@')
        .context("R1_POSTGRES_URL must include user/password and host")?;
    let (user, password) = credentials
        .split_once(':')
        .context("R1_POSTGRES_URL must include a password")?;
    let (address, database) = address_and_database
        .split_once('/')
        .context("R1_POSTGRES_URL must include a database")?;
    let (host, port) = match address.rsplit_once(':') {
        Some((host, port)) => (
            host.to_owned(),
            port.parse().context("parsing PostgreSQL port")?,
        ),
        None => (address.to_owned(), 5432),
    };
    Ok(ControlReplicationConfig {
        host,
        port,
        user: user.to_owned(),
        password: password.to_owned(),
        database: database.split('?').next().unwrap_or(database).to_owned(),
        initial_lsn: 0,
        frame_log_dir: frame_log_dir.as_ref().to_path_buf(),
        segment_max_bytes: 1 << 20,
    })
}

async fn application_row_digest(client: &Client) -> Result<String> {
    let rows = client
        .query(
            "
            SELECT kind, identity, payload FROM (
                SELECT 'customer'::text AS kind, customer_id::text AS identity,
                    concat_ws('|', tenant_id, segment, email_domain, profile::text, created_at::text) AS payload
                FROM r1.customers WHERE customer_id >= 10000000001 AND customer_id < 10000001000
                UNION ALL
                SELECT 'order', order_id::text,
                    concat_ws('|', tenant_id, customer_id, status, channel, amount_cents, attributes::text, updated_at::text)
                FROM r1.orders WHERE order_id >= 20000000001 AND order_id < 20000001000
                UNION ALL
                SELECT 'event', event_id::text,
                    concat_ws('|', order_id, tenant_id, event_type, metadata::text, event_at::text)
                FROM r1.order_events WHERE event_id >= 30000000001 AND event_id < 30000001000
            ) rows ORDER BY kind, identity
            ",
            &[],
        )
        .await?;
    let mut digest = Sha256::new();
    for row in rows {
        for column in 0..3 {
            let value: String = row.get(column);
            digest.update(value.len().to_be_bytes());
            digest.update(value.as_bytes());
        }
    }
    Ok(format!("{:x}", digest.finalize()))
}
