//! Service-backed R1 writer coverage. Task 11 runs this against the Compose source.

use graydb_r1::{CommittedLedger, LedgerEntry};
use sha2::{Digest, Sha256};
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "requires the Task 11 PostgreSQL logical-replication service environment"]
async fn commits_mixed_workload_recovers_unknown_commit_and_preserves_ledger() {
    let url =
        std::env::var("GRAYDB_R1_POSTGRES_URL").expect("Task 11 supplies GRAYDB_R1_POSTGRES_URL");
    let (mut client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });

    client
        .batch_execute(
            "
            CREATE SCHEMA IF NOT EXISTS r1_control;
            CREATE TABLE IF NOT EXISTS r1_control.tx_marker (
                sequence bigint PRIMARY KEY,
                operation_sha256 text NOT NULL
            );
            DROP TABLE IF EXISTS r1_task6_source;
            DROP TABLE IF EXISTS r1_task6_replay;
            CREATE TABLE r1_task6_source (sequence bigint PRIMARY KEY, value bigint NOT NULL);
            CREATE TABLE r1_task6_replay (sequence bigint PRIMARY KEY, value bigint NOT NULL);
            DELETE FROM r1_control.tx_marker WHERE sequence BETWEEN 9000001 AND 9000100;
            ",
        )
        .await
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let mut ledger = CommittedLedger::create(dir.path()).unwrap();
    let mut deferred_ledger = Vec::new();
    let mut plans = Vec::new();
    for sequence in 1..=100_u64 {
        let marker_sequence = 9_000_000 + sequence;
        let value = ((sequence * 17) ^ 0x5a5a) as i64;
        let hash = operation_hash(sequence, value);
        let transaction = client.transaction().await.unwrap();
        let operation_kind = sequence % 3;
        match operation_kind {
            // insert
            0 => {
                transaction
                    .execute(
                        "INSERT INTO r1_task6_source (sequence, value) VALUES ($1, $2)",
                        &[&(marker_sequence as i64), &value],
                    )
                    .await
                    .unwrap();
            }
            // insert + update inside one transaction
            1 => {
                transaction
                    .execute(
                        "INSERT INTO r1_task6_source (sequence, value) VALUES ($1, $2)",
                        &[&(marker_sequence as i64), &value],
                    )
                    .await
                    .unwrap();
                transaction
                    .execute(
                        "UPDATE r1_task6_source SET value = value + 1 WHERE sequence = $1",
                        &[&(marker_sequence as i64)],
                    )
                    .await
                    .unwrap();
            }
            // insert + delete inside one transaction
            _ => {
                transaction
                    .execute(
                        "INSERT INTO r1_task6_source (sequence, value) VALUES ($1, $2)",
                        &[&(marker_sequence as i64), &value],
                    )
                    .await
                    .unwrap();
                transaction
                    .execute(
                        "DELETE FROM r1_task6_source WHERE sequence = $1",
                        &[&(marker_sequence as i64)],
                    )
                    .await
                    .unwrap();
            }
        }
        transaction
            .execute(
                "INSERT INTO r1_control.tx_marker (sequence, operation_sha256) VALUES ($1, $2)",
                &[&(marker_sequence as i64), &hash],
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let lsn = current_lsn(&client).await;
        let entry = LedgerEntry {
            sequence,
            xid: 0,
            source_lsn: lsn,
            operation_sha256: hash.clone(),
            committed_unix_ms: sequence as u128,
            previous_entry_sha256: ledger
                .entries()
                .last()
                .map(|entry| entry.entry_sha256.clone())
                .unwrap_or_default(),
            entry_sha256: String::new(),
        };
        // Simulate a kill after SQL COMMIT but before its ledger append. The
        // resumed writer must not append sequence 58 until #57 is classified.
        if sequence >= 57 {
            deferred_ledger.push((entry, marker_sequence, hash.clone()));
        } else {
            ledger.append(entry).unwrap();
        }
        plans.push((marker_sequence, value, hash, operation_kind));
    }

    drop(ledger);
    let mut ledger = CommittedLedger::resume(dir.path()).unwrap();
    let (pending_entry, marker_sequence, expected_hash) = deferred_ledger.remove(0);
    let observed_hash: String = client
        .query_one(
            "SELECT operation_sha256 FROM r1_control.tx_marker WHERE sequence = $1",
            &[&(marker_sequence as i64)],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        observed_hash, expected_hash,
        "unknown commit must be classified by its marker"
    );
    // The control marker is the sole proof for sequence 57. Rebuild the ledger in
    // strict sequence order; any gap or duplicate causes CommittedLedger to reject.
    ledger.append(pending_entry).unwrap();
    for (entry, _, _) in deferred_ledger {
        ledger.append(entry).unwrap();
    }

    for (sequence, value, _, operation_kind) in &plans {
        match operation_kind {
            0 => {
                client
                    .execute(
                        "INSERT INTO r1_task6_replay (sequence, value) VALUES ($1, $2)",
                        &[&(*sequence as i64), value],
                    )
                    .await
                    .unwrap();
            }
            1 => {
                client
                    .execute(
                        "INSERT INTO r1_task6_replay (sequence, value) VALUES ($1, $2)",
                        &[&(*sequence as i64), &(value + 1)],
                    )
                    .await
                    .unwrap();
            }
            _ => {}
        }
    }
    let unique_markers: i64 = client
        .query_one(
            "SELECT count(DISTINCT sequence) FROM r1_control.tx_marker WHERE sequence BETWEEN 9000001 AND 9000100",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(unique_markers, 100);
    assert_eq!(ledger.entries().len(), 100);
    assert_eq!(ledger.next_sequence(), 101);
    assert_eq!(
        table_digest(&client, "r1_task6_source").await,
        table_digest(&client, "r1_task6_replay").await
    );
}

async fn current_lsn(client: &tokio_postgres::Client) -> u64 {
    let value: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    graydb_ingest::repl::parse_lsn(&value).unwrap()
}

async fn table_digest(client: &tokio_postgres::Client, table: &str) -> String {
    let rows = client
        .query(
            &format!("SELECT sequence, value FROM {table} ORDER BY sequence"),
            &[],
        )
        .await
        .unwrap();
    let mut hash = Sha256::new();
    for row in rows {
        hash.update(row.get::<_, i64>(0).to_be_bytes());
        hash.update(row.get::<_, i64>(1).to_be_bytes());
    }
    format!("{:x}", hash.finalize())
}

fn operation_hash(sequence: u64, value: i64) -> String {
    let mut hash = Sha256::new();
    hash.update(sequence.to_be_bytes());
    hash.update(value.to_be_bytes());
    format!("{:x}", hash.finalize())
}
