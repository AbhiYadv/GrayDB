//! R1's control stream is deliberately separate from the application publication.
//! A transaction reaches the workload ledger only after its marker is decoded from
//! the control stream's *durable* commit frame.  Receipt on a socket is never a
//! commit proof and PostgreSQL's commit LSN, not an interior row LSN, is the
//! checkpoint used by the benchmark.

use crate::ledger::{CommitState, CommittedLedger, IntentLog, LedgerEntry};
use crate::workload::{Operation, TransactionPlan, WorkloadPlanner};
use anyhow::{anyhow, bail, Context, Result};
use graydb_ingest::repl::{parse_lsn, ReplClient, ReplMsg};
use graydb_log::{Frame, FrameLog};
use graydb_registry::decoder::StreamDecoder;
use graydb_registry::{Op, TypedChange};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tokio_postgres::{Client, NoTls};

pub const CONTROL_SLOT: &str = "graydb_r1_control_slot";
pub const CONTROL_PUBLICATION: &str = "graydb_r1_control_pub";
pub const CONTROL_MARKER_TABLE: &str = "r1_control.tx_marker";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerCommit {
    pub sequence: u64,
    pub xid: u32,
    /// PostgreSQL pgoutput Commit.end_lsn.  This is the only source LSN used for
    /// workload checkpoints and slot acknowledgement.
    pub source_lsn: u64,
    pub operation_sha256: String,
}

/// Classification of an outcome that may have lost its write connection. This
/// boundary is deliberately independent from `ApplicationWriter`'s write client:
/// a client which failed during COMMIT cannot be trusted to answer the marker
/// query that decides whether a retry is safe.
#[async_trait::async_trait]
pub trait CommitRecovery: Send + Sync {
    async fn classify_committed_plan(&self, plan: &TransactionPlan) -> Result<CommitState>;
}

/// Opens a separate PostgreSQL connection for each uncertain-commit
/// classification. The short-lived client is closed after the marker query so it
/// cannot accidentally become the application's write connection.
#[derive(Debug, Clone)]
pub struct PostgresCommitRecovery {
    database_url: String,
}

impl PostgresCommitRecovery {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
        }
    }
}

#[async_trait::async_trait]
impl CommitRecovery for PostgresCommitRecovery {
    async fn classify_committed_plan(&self, plan: &TransactionPlan) -> Result<CommitState> {
        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .context("opening fresh recovery connection")?;
        let driver = tokio::spawn(async move { connection.await });
        let row = client
            .query_opt(
                "SELECT operation_sha256 FROM r1_control.tx_marker WHERE sequence = $1",
                &[&(plan.sequence as i64)],
            )
            .await
            .context("querying control marker from fresh recovery connection")?;
        drop(client);
        driver
            .await
            .context("joining fresh recovery connection")?
            .context("fresh recovery connection failed")?;
        match row {
            Some(row) if row.get::<_, String>(0) == plan.operation_sha256 => {
                Ok(CommitState::Committed)
            }
            Some(_) => bail!("control marker hash disagrees with transaction intent"),
            None => Ok(CommitState::UnknownCommit),
        }
    }
}

/// Incrementally maps a marker row to the commit-end LSN that made its transaction
/// visible. `StreamDecoder` buffers all changes until a Commit frame, so marker
/// rows can never escape early.
pub struct ControlLsnMapper {
    decoder: StreamDecoder,
}

impl Default for ControlLsnMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl ControlLsnMapper {
    pub fn new() -> Self {
        Self {
            decoder: StreamDecoder::new(),
        }
    }

    pub fn feed(&mut self, frame: Frame) -> Result<Option<LedgerCommit>> {
        let batch = self.decoder.feed(&[frame])?;
        if batch.last_commit_lsn == 0 {
            return Ok(None);
        }

        let mut marker = None;
        for change in batch
            .changes
            .iter()
            .filter(|change| change.table == CONTROL_MARKER_TABLE)
        {
            if marker.is_some() {
                bail!("multiple r1 control markers in one transaction");
            }
            marker = Some(marker_from_change(change, batch.last_commit_lsn)?);
        }
        Ok(marker)
    }

    pub fn abort_open_transaction(&mut self) {
        self.decoder.abort_open_txn();
    }
}

fn marker_from_change(change: &TypedChange, commit_lsn: u64) -> Result<LedgerCommit> {
    anyhow::ensure!(change.op == Op::Insert, "control marker must be an insert");
    anyhow::ensure!(
        change.commit_lsn == commit_lsn,
        "control marker commit LSN disagrees with decoded batch"
    );
    let values = change
        .new
        .as_ref()
        .context("control marker has no new row image")?;
    let field = |name: &str| -> Result<String> {
        let (_, value) = values
            .iter()
            .find(|(column, _)| column == name)
            .with_context(|| format!("control marker is missing {name}"))?;
        match value {
            graydb_registry::pgoutput::TupleValue::Text(value) => Ok(value.clone()),
            _ => bail!("control marker column {name} is not a text pgoutput value"),
        }
    };
    Ok(LedgerCommit {
        sequence: field("sequence")?
            .parse()
            .context("parsing control marker sequence")?,
        xid: change.xid,
        source_lsn: commit_lsn,
        operation_sha256: field("operation_sha256")?,
    })
}

#[derive(Debug, Clone)]
pub struct ControlReplicationConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub initial_lsn: u64,
    pub frame_log_dir: PathBuf,
    pub segment_max_bytes: u64,
}

/// Owns the control slot and writes each received frame before decoding it. Frames
/// are held until their transaction's Commit has been fsync'd, then decoded as a
/// single durable group. The caller consumes mapped commits through `mapped_tx`.
pub async fn run_control_replication(
    config: ControlReplicationConfig,
    mapped_tx: mpsc::Sender<LedgerCommit>,
    stop: watch::Receiver<bool>,
) -> Result<()> {
    run_control_replication_with_ready(config, mapped_tx, stop, None).await
}

/// Variant used by service integration tests and controllers which need an explicit
/// guarantee that the control slot is streaming before the first application
/// transaction is submitted.
pub async fn run_control_replication_with_ready(
    config: ControlReplicationConfig,
    mapped_tx: mpsc::Sender<LedgerCommit>,
    mut stop: watch::Receiver<bool>,
    ready: Option<tokio::sync::oneshot::Sender<std::result::Result<(), String>>>,
) -> Result<()> {
    let mut ready = ready;
    let mut repl = match ReplClient::connect(
        &config.host,
        config.port,
        &config.user,
        &config.password,
        &config.database,
    )
    .await
    {
        Ok(repl) => repl,
        Err(error) => {
            if let Some(ready) = ready.take() {
                let _ = ready.send(Err(error.to_string()));
            }
            return Err(error);
        }
    };
    let snapshot = match repl.create_slot_with_snapshot(CONTROL_SLOT).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            if let Some(ready) = ready.take() {
                let _ = ready.send(Err(error.to_string()));
            }
            return Err(error);
        }
    };
    let start_lsn = if config.initial_lsn == 0 {
        parse_lsn(&snapshot.consistent_point)?
    } else {
        config.initial_lsn
    };
    if let Err(error) = repl
        .start_replication(CONTROL_SLOT, CONTROL_PUBLICATION, start_lsn)
        .await
    {
        if let Some(ready) = ready.take() {
            let _ = ready.send(Err(error.to_string()));
        }
        return Err(error);
    }
    if let Some(ready) = ready.take() {
        let _ = ready.send(Ok(()));
    }

    let mut log = FrameLog::create(&config.frame_log_dir, config.segment_max_bytes).await?;
    let mut mapper = ControlLsnMapper::new();
    let mut pending = Vec::new();
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() { break; }
            }
            message = repl.next_replication_message() => match message? {
                ReplMsg::XLogData { wal_start, payload } => {
                    let (lsn_end, txn_complete) = pgoutput_commit_end_lsn(&payload)
                        .map(|lsn| (lsn, true))
                        .unwrap_or((wal_start, false));
                    let seq = log.append(wal_start, lsn_end, txn_complete, payload.clone()).await?;
                    pending.push(Frame { seq, lsn_start: wal_start, lsn_end, txn_complete, payload });
                    if txn_complete {
                        // FrameLog::append fsyncs a commit frame before returning.
                        for frame in pending.drain(..) {
                            if let Some(commit) = mapper.feed(frame)? {
                                mapped_tx.send(commit).await.map_err(|_| anyhow!("control commit receiver dropped"))?;
                            }
                        }
                        let durable = log.durable_now();
                        anyhow::ensure!(durable.valid && durable.lsn == lsn_end, "control frame log has no durable commit mark");
                        repl.send_standby_status(durable.lsn, false).await?;
                    }
                }
                ReplMsg::Keepalive { wal_end: _, reply_requested } if reply_requested => {
                    let durable = log.durable_now();
                    repl.send_standby_status(if durable.valid { durable.lsn } else { start_lsn }, false).await?;
                }
                ReplMsg::Keepalive { .. } => {}
            }
        }
    }
    let durable = log.durable_now();
    if durable.valid {
        repl.send_standby_status(durable.lsn, false).await.ok();
    }
    repl.close().await.ok();
    Ok(())
}

fn pgoutput_commit_end_lsn(payload: &[u8]) -> Option<u64> {
    if payload.len() >= 26 && payload.first() == Some(&b'C') {
        Some(u64::from_be_bytes(payload[10..18].try_into().ok()?))
    } else {
        None
    }
}

/// Executes deterministic SQL plans. The writer does not append a ledger entry at
/// SQL commit time; it waits for the independently durable control publication.
pub struct ApplicationWriter {
    client: Client,
    recovery: Arc<dyn CommitRecovery>,
    planner: WorkloadPlanner,
    intents: IntentLog,
    ledger: CommittedLedger,
    mapped_rx: mpsc::Receiver<LedgerCommit>,
    pending_maps: BTreeMap<u64, LedgerCommit>,
}

impl ApplicationWriter {
    pub fn new(
        client: Client,
        recovery: Arc<dyn CommitRecovery>,
        planner: WorkloadPlanner,
        intents: IntentLog,
        ledger: CommittedLedger,
        mapped_rx: mpsc::Receiver<LedgerCommit>,
    ) -> Self {
        Self {
            client,
            recovery,
            planner,
            intents,
            ledger,
            mapped_rx,
            pending_maps: BTreeMap::new(),
        }
    }

    pub fn ledger(&self) -> &CommittedLedger {
        &self.ledger
    }

    /// Models a process death after SQL COMMIT: ownership of the write client is
    /// discarded while the independently-owned control-stream receiver survives
    /// and is handed to a newly constructed writer.
    pub fn into_mapped_receiver(self) -> mpsc::Receiver<LedgerCommit> {
        self.mapped_rx
    }

    /// Runs generated plans until `stop` is set. `target_rate` is affected rows per
    /// second; the limiter is also the source of the measured achieved rate.
    pub async fn run(&mut self, target_rate: u64, stop: watch::Receiver<bool>) -> Result<()> {
        let mut limiter = crate::RateLimiter::new(target_rate, 1_000);
        let mut sequence = self.ledger.next_sequence();
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            let plan = self.planner.plan(sequence);
            limiter.acquire(plan.operations.len() as u64).await?;
            self.execute_and_record(&plan).await?;
            sequence += 1;
        }
    }

    pub async fn execute_and_record(&mut self, plan: &TransactionPlan) -> Result<LedgerCommit> {
        match self.ledger.classify(plan) {
            CommitState::Committed => bail!("plan {} is already committed", plan.sequence),
            CommitState::UnknownCommit => {}
        }
        let xid = match self.submit_plan(plan).await {
            Ok(xid) => Some(xid),
            Err(error) => {
                // A disconnect after COMMIT submission is intentionally never retried
                // here. The marker classifies the intent before either accepting it
                // or returning an UnknownCommit to the controller.
                return self.recover_and_record(plan).await.with_context(|| {
                    format!(
                        "SQL transaction outcome unknown; fresh marker classification required before retry: {error:#}"
                    )
                });
            }
        };
        let mapped = self
            .wait_for_mapping(plan.sequence, &plan.operation_sha256)
            .await?;
        if let Some(xid) = xid {
            anyhow::ensure!(
                mapped.xid == xid,
                "control marker xid differs from SQL transaction xid"
            );
        }
        self.append_ledger(&mapped)?;
        Ok(mapped)
    }

    /// Durably records intent and submits its SQL transaction, but deliberately
    /// does not wait for the control stream or append the workload ledger. This is
    /// the explicit crash boundary used by the recovery integration test.
    pub async fn submit_plan(&mut self, plan: &TransactionPlan) -> Result<u32> {
        match self.ledger.classify(plan) {
            CommitState::Committed => bail!("plan {} is already committed", plan.sequence),
            CommitState::UnknownCommit => {}
        }
        self.intents.append(plan)?;
        execute_transaction(&mut self.client, plan).await
    }

    /// Resolves a plan left between SQL COMMIT and ledger append. It always asks a
    /// fresh recovery provider first, and accepts the plan only after the same
    /// sequence/hash is observed through the durable control mapper.
    pub async fn recover_and_record(&mut self, plan: &TransactionPlan) -> Result<LedgerCommit> {
        match self.recovery.classify_committed_plan(plan).await? {
            CommitState::Committed => {
                let mapped = self
                    .wait_for_mapping(plan.sequence, &plan.operation_sha256)
                    .await?;
                self.append_ledger(&mapped)?;
                Ok(mapped)
            }
            CommitState::UnknownCommit => bail!(
                "SQL transaction outcome remains unknown; no retry is permitted before a fresh control marker appears"
            ),
        }
    }

    pub async fn wait_for_mapping(&mut self, sequence: u64, hash: &str) -> Result<LedgerCommit> {
        if let Some(mapped) = self.pending_maps.remove(&sequence) {
            anyhow::ensure!(
                mapped.operation_sha256 == hash,
                "mapped operation hash mismatch"
            );
            return Ok(mapped);
        }
        loop {
            let mapped = self
                .mapped_rx
                .recv()
                .await
                .context("control mapper stopped before transaction was visible")?;
            if mapped.sequence == sequence {
                anyhow::ensure!(
                    mapped.operation_sha256 == hash,
                    "mapped operation hash mismatch"
                );
                return Ok(mapped);
            }
            if mapped.sequence < sequence {
                bail!(
                    "control mapping sequence {} is behind requested {}",
                    mapped.sequence,
                    sequence
                );
            }
            if self.pending_maps.insert(mapped.sequence, mapped).is_some() {
                bail!("duplicate control mapping sequence received");
            }
        }
    }

    fn append_ledger(&mut self, mapped: &LedgerCommit) -> Result<()> {
        let previous_entry_sha256 = self
            .ledger
            .entries()
            .last()
            .map(|entry| entry.entry_sha256.clone())
            .unwrap_or_default();
        self.ledger.append(LedgerEntry {
            sequence: mapped.sequence,
            xid: mapped.xid,
            source_lsn: mapped.source_lsn,
            operation_sha256: mapped.operation_sha256.clone(),
            committed_unix_ms: unix_ms(),
            previous_entry_sha256,
            entry_sha256: String::new(),
        })
    }
}

async fn execute_transaction(client: &mut Client, plan: &TransactionPlan) -> Result<u32> {
    let transaction = client.transaction().await?;
    for operation in &plan.operations {
        execute_operation(&transaction, operation).await?;
    }
    transaction
        .execute(
            "INSERT INTO r1_control.tx_marker (sequence, operation_sha256) VALUES ($1, $2)",
            &[&(plan.sequence as i64), &plan.operation_sha256],
        )
        .await?;
    let xid = transaction
        .query_one("SELECT txid_current()::text", &[])
        .await?
        .get::<_, String>(0)
        .parse()
        .context("parsing txid_current")?;
    transaction.commit().await?;
    Ok(xid)
}

async fn execute_operation(
    transaction: &tokio_postgres::Transaction<'_>,
    operation: &Operation,
) -> Result<()> {
    match operation {
        Operation::InsertCustomer(row) => {
            transaction.execute("INSERT INTO r1.customers (customer_id, tenant_id, segment, email_domain, profile, created_at) VALUES ($1,$2,$3,$4,$5::jsonb,to_timestamp($6::double precision / 1000000))", &[&(row.customer_id as i64), &(row.tenant_id as i64), &row.segment, &row.email_domain, &row.profile_json, &(row.created_at_micros as f64)]).await?;
        }
        Operation::InsertOrder(row) => {
            transaction.execute("INSERT INTO r1.orders (order_id, tenant_id, customer_id, status, channel, amount_cents, created_at, updated_at, attributes) VALUES ($1,$2,$3,$4,$5,$6,to_timestamp($7::double precision / 1000000),to_timestamp($8::double precision / 1000000),$9::jsonb)", &[&(row.order_id as i64), &(row.tenant_id as i64), &(row.customer_id as i64), &row.status, &row.channel, &row.amount_cents, &(row.created_at_micros as f64), &(row.updated_at_micros as f64), &row.attributes_json]).await?;
        }
        Operation::InsertOrderEvent(row) => {
            transaction.execute("INSERT INTO r1.order_events (event_id, order_id, tenant_id, event_type, event_at, metadata) VALUES ($1,$2,$3,$4,to_timestamp($5::double precision / 1000000),$6::jsonb)", &[&(row.event_id as i64), &(row.order_id as i64), &(row.tenant_id as i64), &row.event_type, &(row.event_at_micros as f64), &row.metadata_json]).await?;
        }
        Operation::UpdateCustomer {
            customer_id,
            tenant_id,
            segment,
            email_domain,
            profile_json,
            created_at_micros,
        } => {
            transaction.execute("UPDATE r1.customers SET tenant_id=$2, segment=$3, email_domain=$4, profile=$5::jsonb, created_at=to_timestamp($6::double precision / 1000000) WHERE customer_id=$1", &[&(*customer_id as i64), &(*tenant_id as i64), segment, email_domain, profile_json, &(*created_at_micros as f64)]).await?;
        }
        Operation::UpdateOrder {
            order_id,
            tenant_id,
            customer_id,
            status,
            channel,
            amount_cents,
            created_at_micros,
            updated_at_micros,
            attributes_json,
        } => {
            transaction.execute("UPDATE r1.orders SET tenant_id=$2, customer_id=$3, status=$4, channel=$5, amount_cents=$6, created_at=to_timestamp($7::double precision / 1000000), updated_at=to_timestamp($8::double precision / 1000000), attributes=$9::jsonb WHERE order_id=$1", &[&(*order_id as i64), &(*tenant_id as i64), &(*customer_id as i64), status, channel, amount_cents, &(*created_at_micros as f64), &(*updated_at_micros as f64), attributes_json]).await?;
        }
        Operation::DeleteOrder {
            order_id,
            tenant_id,
        } => {
            transaction
                .execute(
                    "DELETE FROM r1.orders WHERE order_id=$1 AND tenant_id=$2",
                    &[&(*order_id as i64), &(*tenant_id as i64)],
                )
                .await?;
        }
        Operation::DeleteOrderEvent {
            event_id,
            order_id,
            tenant_id,
        } => {
            transaction.execute("DELETE FROM r1.order_events WHERE event_id=$1 AND order_id=$2 AND tenant_id=$3", &[&(*event_id as i64), &(*order_id as i64), &(*tenant_id as i64)]).await?;
        }
    }
    Ok(())
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayMapEntry {
    pub logical_sequence: u64,
    pub original_source_lsn: u64,
    pub replay_source_lsn: u64,
    pub operation_sha256: String,
}

pub struct ReplayMap {
    path: PathBuf,
    entries: Vec<ReplayMapEntry>,
    /// Byte offset of the verified prefix of `replay-map.jsonl` (append-only).
    loaded_bytes: u64,
}

impl ReplayMap {
    pub fn create(dir: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(dir.as_ref())?;
        let path = dir.as_ref().join("replay-map.jsonl");
        OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path,
            entries: Vec::new(),
            loaded_bytes: 0,
        })
    }

    pub fn resume(dir: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(dir.as_ref())?;
        let path = dir.as_ref().join("replay-map.jsonl");
        OpenOptions::new().create(true).append(true).open(&path)?;
        let mut map = Self {
            path,
            entries: Vec::new(),
            loaded_bytes: 0,
        };
        map.refresh()?;
        Ok(map)
    }

    /// Incrementally loads replay-map lines appended since the last refresh.
    /// A final line that is not yet newline-terminated is left for the next
    /// refresh; an interior corrupt line is a hard error.
    pub fn refresh(&mut self) -> Result<()> {
        let mut file = std::fs::File::open(&self.path)?;
        let file_len = file.metadata()?.len();
        if file_len < self.loaded_bytes {
            return Err(anyhow!(
                "replay map shrank from {} to {} bytes; append-only invariant broken",
                self.loaded_bytes,
                file_len
            ));
        }
        if file_len == self.loaded_bytes {
            return Ok(());
        }
        use std::io::{BufRead, Seek, SeekFrom};
        file.seek(SeekFrom::Start(self.loaded_bytes))?;
        let mut reader = std::io::BufReader::new(file);
        let mut consumed = self.loaded_bytes;
        let mut line = String::new();
        loop {
            let line_start = consumed;
            line.clear();
            let read = reader.read_line(&mut line)?;
            if read == 0 {
                break;
            }
            consumed += read as u64;
            let trimmed = line.trim_end_matches(['\n', '\r']);
            if trimmed.is_empty() {
                continue;
            }
            let entry: ReplayMapEntry = match serde_json::from_str(trimmed) {
                Ok(entry) => entry,
                Err(error) if consumed >= file_len && !line.ends_with('\n') => {
                    // Unterminated final line: the replayer has not finished
                    // this append.  Leave it for the next refresh.  A corrupt
                    // newline-terminated line stays a hard error.
                    let _ = error;
                    consumed = line_start;
                    break;
                }
                Err(error) => return Err(error).context("replay map line is corrupt"),
            };
            self.validate_next(&entry)?;
            self.entries.push(entry);
        }
        self.loaded_bytes = consumed;
        Ok(())
    }

    pub fn push(&mut self, entry: ReplayMapEntry) -> Result<()> {
        self.validate_next(&entry)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(file, "{}", serde_json::to_string(&entry)?)?;
        file.sync_data()?;
        self.entries.push(entry);
        Ok(())
    }

    fn validate_next(&self, entry: &ReplayMapEntry) -> Result<()> {
        let expected = self
            .entries
            .last()
            .map(|entry| entry.logical_sequence + 1)
            .unwrap_or(1);
        anyhow::ensure!(
            entry.logical_sequence == expected,
            "replay map sequence {}, expected {}",
            entry.logical_sequence,
            expected
        );
        anyhow::ensure!(
            !entry.operation_sha256.is_empty(),
            "replay map operation hash is required"
        );
        Ok(())
    }

    pub fn entries(&self) -> &[ReplayMapEntry] {
        &self.entries
    }
}

/// Validates immutable committed intent before a restored-source replay. Database
/// execution is delegated to `ApplicationWriter`; this type guards the sequence and
/// hash contract and persists the original-to-replay LSN correspondence.
pub struct WorkloadReplayer {
    replay_map: ReplayMap,
}

impl WorkloadReplayer {
    pub fn new(replay_map: ReplayMap) -> Self {
        Self { replay_map }
    }
    pub fn replay_map(&self) -> &ReplayMap {
        &self.replay_map
    }

    pub fn replay(&mut self, entries: &[(TransactionPlan, LedgerEntry, u64)]) -> Result<()> {
        let mut expected_sequence = self
            .replay_map
            .entries()
            .last()
            .map(|entry| entry.logical_sequence + 1)
            .unwrap_or(1);
        for (plan, original, replay_lsn) in entries {
            anyhow::ensure!(
                plan.sequence == original.sequence,
                "replay plan sequence differs from ledger"
            );
            anyhow::ensure!(
                plan.operation_sha256 == original.operation_sha256,
                "replay operation hash differs from committed ledger"
            );
            let candidate = ReplayMapEntry {
                logical_sequence: plan.sequence,
                original_source_lsn: original.source_lsn,
                replay_source_lsn: *replay_lsn,
                operation_sha256: plan.operation_sha256.clone(),
            };
            anyhow::ensure!(
                candidate.logical_sequence == expected_sequence,
                "replay sequence {}, expected {}",
                candidate.logical_sequence,
                expected_sequence
            );
            anyhow::ensure!(
                !candidate.operation_sha256.is_empty(),
                "replay operation hash is required"
            );
            expected_sequence += 1;
        }
        for (plan, original, replay_lsn) in entries {
            self.replay_map.push(ReplayMapEntry {
                logical_sequence: plan.sequence,
                original_source_lsn: original.source_lsn,
                replay_source_lsn: *replay_lsn,
                operation_sha256: plan.operation_sha256.clone(),
            })?;
        }
        Ok(())
    }

    pub fn into_replay_map(self) -> ReplayMap {
        self.replay_map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, Bytes};
    use tempfile::tempdir;

    fn frame(seq: u64, lsn: u64, payload: Vec<u8>, commit: bool) -> Frame {
        Frame {
            seq,
            lsn_start: lsn,
            lsn_end: lsn,
            txn_complete: commit,
            payload: Bytes::from(payload),
        }
    }
    fn begin(xid: u32) -> Vec<u8> {
        let mut b = vec![b'B'];
        b.put_u64(0);
        b.put_i64(0);
        b.put_u32(xid);
        b
    }
    fn commit(end: u64) -> Vec<u8> {
        let mut b = vec![b'C'];
        b.put_u8(0);
        b.put_u64(end - 1);
        b.put_u64(end);
        b.put_i64(0);
        b
    }
    fn relation(relid: u32, schema: &str, table: &str, fields: &[&str]) -> Vec<u8> {
        let mut b = vec![b'R'];
        b.put_u32(relid);
        b.extend(schema.as_bytes());
        b.put_u8(0);
        b.extend(table.as_bytes());
        b.put_u8(0);
        b.put_u8(b'd');
        b.put_u16(fields.len() as u16);
        for field in fields {
            b.put_u8(0);
            b.extend(field.as_bytes());
            b.put_u8(0);
            b.put_u32(25);
            b.put_i32(-1);
        }
        b
    }
    fn insert(relid: u32, values: &[&str]) -> Vec<u8> {
        let mut b = vec![b'I'];
        b.put_u32(relid);
        b.put_u8(b'N');
        b.put_u16(values.len() as u16);
        for value in values {
            b.put_u8(b't');
            b.put_u32(value.len() as u32);
            b.extend(value.as_bytes());
        }
        b
    }
    fn marker_transaction_frames(sequence: u64, hash: &str, end_lsn: u64) -> Vec<Frame> {
        vec![
            frame(
                0,
                1,
                relation(
                    42,
                    "r1_control",
                    "tx_marker",
                    &["sequence", "operation_sha256"],
                ),
                false,
            ),
            frame(1, 2, relation(43, "r1", "orders", &["order_id"]), false),
            frame(2, 3, begin(9001), false),
            frame(3, 4, insert(42, &[&sequence.to_string(), hash]), false),
            frame(4, 5, insert(43, &["20000000001"]), false),
            frame(5, end_lsn, commit(end_lsn), true),
        ]
    }

    #[test]
    fn marker_becomes_visible_only_at_commit_end_lsn() {
        let mut mapper = ControlLsnMapper::new();
        for frame in marker_transaction_frames(77, "abc", 0xA000_0042) {
            let complete = frame.txn_complete;
            let mapped = mapper.feed(frame).unwrap();
            if complete {
                assert_eq!(
                    mapped.unwrap(),
                    LedgerCommit {
                        sequence: 77,
                        xid: 9001,
                        source_lsn: 0xA000_0042,
                        operation_sha256: "abc".into()
                    }
                );
            } else {
                assert!(mapped.is_none());
            }
        }
    }

    #[test]
    fn replay_map_refuses_gaps_and_hash_mismatches() {
        let dir = tempdir().unwrap();
        let planner = WorkloadPlanner::new(20260901);
        let plan = planner.plan(1);
        let ledger = LedgerEntry {
            sequence: 1,
            xid: 1,
            source_lsn: 10,
            operation_sha256: plan.operation_sha256.clone(),
            committed_unix_ms: 0,
            previous_entry_sha256: String::new(),
            entry_sha256: String::new(),
        };
        let mut replayer = WorkloadReplayer::new(ReplayMap::create(dir.path()).unwrap());
        replayer.replay(&[(plan.clone(), ledger, 20)]).unwrap();
        let gap = planner.plan(3);
        let gap_ledger = LedgerEntry {
            sequence: 3,
            xid: 3,
            source_lsn: 30,
            operation_sha256: gap.operation_sha256.clone(),
            committed_unix_ms: 0,
            previous_entry_sha256: String::new(),
            entry_sha256: String::new(),
        };
        assert!(replayer.replay(&[(gap, gap_ledger, 40)]).is_err());
        let mismatch = LedgerEntry {
            sequence: 2,
            xid: 2,
            source_lsn: 20,
            operation_sha256: "wrong".into(),
            committed_unix_ms: 0,
            previous_entry_sha256: String::new(),
            entry_sha256: String::new(),
        };
        assert!(replayer.replay(&[(planner.plan(2), mismatch, 30)]).is_err());
    }

    #[test]
    fn replay_map_resume_validates_without_reappending_entries() {
        let dir = tempdir().unwrap();
        let mut map = ReplayMap::create(dir.path()).unwrap();
        map.push(ReplayMapEntry {
            logical_sequence: 1,
            original_source_lsn: 10,
            replay_source_lsn: 20,
            operation_sha256: "abc".into(),
        })
        .unwrap();
        drop(map);
        assert_eq!(ReplayMap::resume(dir.path()).unwrap().entries().len(), 1);
        let contents = std::fs::read_to_string(dir.path().join("replay-map.jsonl")).unwrap();
        assert_eq!(contents.lines().count(), 1);
    }

    #[test]
    fn replay_map_refresh_loads_appended_entries() {
        let dir = tempdir().unwrap();
        let mut map = ReplayMap::create(dir.path()).unwrap();
        map.push(ReplayMapEntry {
            logical_sequence: 1,
            original_source_lsn: 10,
            replay_source_lsn: 20,
            operation_sha256: "abc".into(),
        })
        .unwrap();
        let mut cached = ReplayMap::resume(dir.path()).unwrap();

        map.push(ReplayMapEntry {
            logical_sequence: 2,
            original_source_lsn: 30,
            replay_source_lsn: 40,
            operation_sha256: "def".into(),
        })
        .unwrap();

        cached.refresh().unwrap();
        assert_eq!(cached.entries().len(), 2);
        assert_eq!(cached.entries()[1].replay_source_lsn, 40);
        // No new lines: refresh is a no-op.
        cached.refresh().unwrap();
        assert_eq!(cached.entries().len(), 2);
    }
}
