//! ClickHouse CDC representation for R1 Phase 1 (spec section 10).
//!
//! One immutable version row per PostgreSQL row change, written as one
//! JSONEachRow batch per committed transaction. Exactness at a target LSN is a
//! query-time reduction: keep versions with `_source_lsn <= target`, pick the
//! greatest `_version` per primary key, drop tombstones — see
//! `bench/r1/queries/clickhouse/*.sql` and `Version`.

use crate::adapter::{EngineAdapter, EngineStatus, QueryInvocation, QueryResult};
use crate::contracts::EngineKind;
use crate::query::{QueryId, QueryParameters};
use anyhow::{anyhow, bail, Context, Result};
use graydb_ingest::repl::ReplClient;
use graydb_log::Frame;
use graydb_registry::decoder::StreamDecoder;
use graydb_registry::pgoutput::TupleValue;
use graydb_registry::{Op, TypedChange};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// Marker table recording every applied transaction (operation hash + LSN).
pub const APPLIED_TRANSACTIONS: &str = "r1_meta.applied_transactions";
/// Table-level `SETTINGS non_replicated_deduplication_window` lives in
/// bench/r1/clickhouse.sql: ClickHouse 25.8 rejects it as a query-level
/// setting, and the token window must be server-side for crash retry safety.
const COMPACT_FORMAT: &str = "JSONCompactEachRowWithNamesAndTypes";

/// Monotonic version for one row change: `(lsn << 32) | ordinal`, where `lsn`
/// is the PostgreSQL commit-end LSN and `ordinal` preserves stream order inside
/// the transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Version(u128);

impl Version {
    pub fn from_lsn_ordinal(lsn: u64, ordinal: u32) -> Self {
        Self(((lsn as u128) << 32) | ordinal as u128)
    }

    pub fn as_u128(self) -> u128 {
        self.0
    }
}

struct RawTable {
    qualified: &'static str,
    raw: &'static str,
    columns: &'static [&'static str],
}

const RAW_TABLES: [RawTable; 4] = [
    RawTable {
        qualified: "r1.tenants",
        raw: "r1_tenants_raw",
        columns: &["tenant_id", "region", "plan", "created_at", "settings"],
    },
    RawTable {
        qualified: "r1.customers",
        raw: "r1_customers_raw",
        columns: &[
            "customer_id",
            "tenant_id",
            "segment",
            "email_domain",
            "profile",
            "created_at",
        ],
    },
    RawTable {
        qualified: "r1.orders",
        raw: "r1_orders_raw",
        columns: &[
            "order_id",
            "tenant_id",
            "customer_id",
            "status",
            "channel",
            "amount_cents",
            "created_at",
            "updated_at",
            "attributes",
        ],
    },
    RawTable {
        qualified: "r1.order_events",
        raw: "r1_order_events_raw",
        columns: &[
            "event_id",
            "order_id",
            "tenant_id",
            "event_type",
            "event_at",
            "metadata",
        ],
    },
];

fn raw_table_for(qualified: &str) -> Option<&'static RawTable> {
    RAW_TABLES.iter().find(|t| t.qualified == qualified)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseStatus {
    pub healthy: bool,
    pub version: Option<String>,
    pub applied_lsn: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    SkippedIdempotent,
}

/// The PostgreSQL acknowledgement boundary owned by the CDC sink. The concrete
/// implementation is [`ReplClient`]; the trait keeps the ordering contract
/// directly testable without a live replication socket.
#[async_trait::async_trait]
pub trait ReplicationAcknowledger: Send {
    async fn acknowledge_applied(&mut self, lsn: u64) -> Result<()>;
}

#[async_trait::async_trait]
impl ReplicationAcknowledger for ReplClient {
    async fn acknowledge_applied(&mut self, lsn: u64) -> Result<()> {
        self.send_standby_status(lsn, false).await
    }
}

/// Applies typed pgoutput changes to the raw version tables, one JSONEachRow
/// POST batch per committed transaction, with marker-based idempotency.
pub struct ClickHouseSink {
    client: reqwest::Client,
    base_url: String,
}

/// Production pgoutput boundary for ClickHouse CDC. It owns the incremental
/// [`StreamDecoder`], so the sink receives only a complete decoder-emitted
/// transaction in the exact frame order in which PostgreSQL published it.
pub struct ClickHouseCdcAdapter {
    sink: ClickHouseSink,
    decoder: StreamDecoder,
}

impl ClickHouseCdcAdapter {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::with_sink(ClickHouseSink::new(base_url))
    }

    pub fn with_sink(sink: ClickHouseSink) -> Self {
        Self {
            sink,
            decoder: StreamDecoder::new(),
        }
    }

    /// Feeds raw, contiguous pgoutput frames. Frames before Commit only extend
    /// the decoder buffer and perform no ClickHouse request. At Commit the
    /// decoder supplies one ordered transaction with its commit-end LSN, which
    /// is the sole input accepted by the sink and acknowledgement boundary.
    ///
    /// `operation_sha256` must be the control-stream operation hash associated
    /// with this one expected application transaction. Supplying two commits in
    /// one call is rejected so a hash can never be attached to the wrong commit.
    pub async fn apply_frames<A: ReplicationAcknowledger + ?Sized>(
        &mut self,
        acknowledger: &mut A,
        operation_sha256: &str,
        frames: &[Frame],
    ) -> Result<Option<ApplyOutcome>> {
        let mut applied = None;
        for frame in frames {
            let batch = self.decoder.feed(std::slice::from_ref(frame))?;
            if batch.txns == 0 {
                anyhow::ensure!(
                    batch.changes.is_empty(),
                    "StreamDecoder emitted changes before transaction Commit"
                );
                continue;
            }
            anyhow::ensure!(
                batch.txns == 1 && batch.last_commit_lsn != 0,
                "expected exactly one committed pgoutput transaction per frame"
            );
            anyhow::ensure!(
                applied.is_none(),
                "apply_frames accepts exactly one committed pgoutput transaction"
            );
            applied = Some(
                self.sink
                    .apply(
                        acknowledger,
                        batch.last_commit_lsn,
                        operation_sha256,
                        &batch.changes,
                    )
                    .await?,
            );
        }
        Ok(applied)
    }

    pub fn abort_open_transaction(&mut self) {
        self.decoder.abort_open_txn();
    }
}

impl ClickHouseSink {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into(),
        }
    }

    /// Runs DDL or other statement batches (bootstrap uses `bench/r1/clickhouse.sql`).
    pub async fn execute(&self, sql: &str) -> Result<()> {
        for statement in split_sql_statements(sql)? {
            self.post(&[], &statement).await?;
        }
        Ok(())
    }

    /// Applies a decoded, committed pgoutput transaction and advances the
    /// PostgreSQL acknowledgement only after every ClickHouse data insert, the
    /// transaction-marker insert, and the marker verification query succeed.
    pub async fn apply<A: ReplicationAcknowledger + ?Sized>(
        &self,
        acknowledger: &mut A,
        commit_lsn: u64,
        operation_sha256: &str,
        changes: &[TypedChange],
    ) -> Result<ApplyOutcome> {
        let outcome = self
            .apply_transaction(commit_lsn, operation_sha256, changes)
            .await?;
        acknowledger.acknowledge_applied(commit_lsn).await?;
        Ok(outcome)
    }

    /// Applies one committed transaction. On retry, a matching marker in
    /// `r1_meta.applied_transactions` is an idempotent skip; a different hash
    /// at the same LSN is a hard error.
    pub async fn apply_transaction(
        &self,
        commit_lsn: u64,
        operation_sha256: &str,
        changes: &[TypedChange],
    ) -> Result<ApplyOutcome> {
        if self
            .transaction_is_applied(commit_lsn, operation_sha256)
            .await?
        {
            return Ok(ApplyOutcome::SkippedIdempotent);
        }
        self.insert_version_rows(commit_lsn, operation_sha256, changes, false)
            .await?;
        self.record_marker(commit_lsn, operation_sha256).await?;
        anyhow::ensure!(
            self.transaction_is_applied(commit_lsn, operation_sha256)
                .await?,
            "transaction marker was not visible after insertion at source LSN {commit_lsn}"
        );
        Ok(ApplyOutcome::Applied)
    }

    /// Applies the initial snapshot: every row is the first version of its key
    /// (`_source_lsn = initial_lsn`, ordinal 0, `_deleted = 0`). A marker with
    /// hash `initial-<lsn>` makes the snapshot checkpoint idempotent and
    /// queryable at its own LSN.
    pub async fn apply_initial_load(
        &self,
        initial_lsn: u64,
        changes: &[TypedChange],
    ) -> Result<()> {
        let hash = format!("initial-{initial_lsn}");
        if self.transaction_is_applied(initial_lsn, &hash).await? {
            return Ok(());
        }
        self.insert_version_rows(initial_lsn, &hash, changes, true)
            .await?;
        self.record_marker(initial_lsn, &hash).await?;
        anyhow::ensure!(
            self.transaction_is_applied(initial_lsn, &hash).await?,
            "initial-load marker was not visible after insertion at source LSN {initial_lsn}"
        );
        Ok(())
    }

    async fn transaction_is_applied(&self, lsn: u64, hash: &str) -> Result<bool> {
        let (body, _) = self
            .post(
                &[],
                &format!(
                    "SELECT operation_sha256 FROM {APPLIED_TRANSACTIONS} \
                     WHERE source_lsn = {lsn} FORMAT TabSeparated"
                ),
            )
            .await?;
        let recorded: Vec<&str> = body
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect();
        if recorded.is_empty() {
            return Ok(false);
        }
        anyhow::ensure!(
            recorded.len() == 1,
            "duplicate transaction markers at source LSN {lsn}: found {}",
            recorded.len()
        );
        anyhow::ensure!(
            recorded[0] == hash,
            "idempotency marker hash mismatch at source LSN {lsn}: recorded {}, expected {hash}",
            recorded[0]
        );
        Ok(true)
    }

    async fn insert_version_rows(
        &self,
        commit_lsn: u64,
        token_id: &str,
        changes: &[TypedChange],
        initial_load: bool,
    ) -> Result<()> {
        anyhow::ensure!(
            !changes.is_empty(),
            "cannot apply an empty ClickHouse transaction"
        );
        let mut rows_by_table: BTreeMap<&'static str, Vec<Value>> = BTreeMap::new();
        for (index, change) in changes.iter().enumerate() {
            anyhow::ensure!(
                change.commit_lsn == 0 || change.commit_lsn == commit_lsn,
                "change for {} carries commit LSN {}, transaction LSN is {}",
                change.table,
                change.commit_lsn,
                commit_lsn
            );
            let raw = raw_table_for(&change.table)
                .with_context(|| format!("unsupported published table {}", change.table))?;
            let ordinal = if initial_load {
                0
            } else {
                u32::try_from(index + 1).context("transaction change ordinal exceeds UInt32")?
            };
            let version = Version::from_lsn_ordinal(commit_lsn, ordinal);
            rows_by_table
                .entry(raw.raw)
                .or_default()
                .push(render_row(raw, change, commit_lsn, ordinal, version)?);
        }
        for (raw, rows) in rows_by_table {
            let mut columns: Vec<&str> = RAW_TABLES
                .iter()
                .find(|t| t.raw == raw)
                .expect("raw table comes from RAW_TABLES")
                .columns
                .to_vec();
            columns.extend(["_source_lsn", "_change_ordinal", "_version", "_deleted"]);
            let mut body = format!(
                "INSERT INTO {raw} ({}) FORMAT JSONEachRow",
                columns.join(", ")
            );
            for row in rows {
                body.push('\n');
                body.push_str(&row.to_string());
            }
            self.post(
                &[
                    ("date_time_input_format", "best_effort".to_string()),
                    ("insert_deduplication_token", format!("{token_id}:{raw}")),
                ],
                &body,
            )
            .await?;
        }
        Ok(())
    }

    async fn record_marker(&self, lsn: u64, hash: &str) -> Result<()> {
        let escaped = hash.replace('\'', "''");
        let body = format!(
            "INSERT INTO {APPLIED_TRANSACTIONS} (operation_sha256, source_lsn, applied_at) \
             VALUES ('{escaped}', {lsn}, now())"
        );
        self.post(
            &[(
                "insert_deduplication_token",
                format!("{escaped}:{APPLIED_TRANSACTIONS}"),
            )],
            &body,
        )
        .await?;
        Ok(())
    }

    async fn post(
        &self,
        params: &[(&str, String)],
        body: &str,
    ) -> Result<(String, reqwest::header::HeaderMap)> {
        clickhouse_request(&self.client, &self.base_url, params, body).await
    }
}

fn split_sql_statements(sql: &str) -> Result<Vec<String>> {
    // Strip `--` line comments first (outside quoted strings): comment text
    // may contain semicolons, which would otherwise split comment-only
    // fragments into statements that ClickHouse sees as "Empty query".
    let mut filtered = String::with_capacity(sql.len());
    let mut quote: Option<char> = None;
    let mut escaped = false;
    let mut characters = sql.chars().peekable();
    while let Some(character) = characters.next() {
        if let Some(active_quote) = quote {
            filtered.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == active_quote {
                quote = None;
            }
            continue;
        }
        match character {
            '\'' | '"' | '`' => {
                quote = Some(character);
                filtered.push(character);
            }
            '-' if characters.peek() == Some(&'-') => {
                for skipped in characters.by_ref() {
                    if skipped == '\n' {
                        filtered.push('\n');
                        break;
                    }
                }
            }
            other => filtered.push(other),
        }
    }
    anyhow::ensure!(
        quote.is_none(),
        "unterminated quoted string in ClickHouse SQL"
    );
    let mut statements = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let mut escaped = false;
    for (index, character) in filtered.char_indices() {
        if let Some(active_quote) = quote {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == active_quote {
                quote = None;
            }
            continue;
        }
        match character {
            '\'' | '"' | '`' => quote = Some(character),
            ';' => {
                let statement = filtered[start..index].trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }
    let trailing = filtered[start..].trim();
    if !trailing.is_empty() {
        statements.push(trailing.to_string());
    }
    Ok(statements)
}

fn render_row(
    raw: &RawTable,
    change: &TypedChange,
    lsn: u64,
    ordinal: u32,
    version: Version,
) -> Result<Value> {
    let image = match change.op {
        Op::Insert | Op::Update => change.new.as_ref(),
        Op::Delete => change.old.as_ref(),
        Op::Truncate => bail!("truncate is not representable as a version row"),
    }
    .with_context(|| {
        format!(
            "{:?} change for {} has no row image",
            change.op, change.table
        )
    })?;
    let mut object = Map::new();
    for column in raw.columns {
        let cell = image
            .iter()
            .find(|(name, _)| name == *column)
            .map(|(_, value)| tuple_json(column, value))
            .transpose()?;
        object.insert((*column).to_string(), cell.unwrap_or(Value::Null));
    }
    object.insert("_source_lsn".into(), json!(lsn));
    object.insert("_change_ordinal".into(), json!(ordinal));
    // UInt128 travels as a JSON string; ClickHouse parses it into the column.
    object.insert("_version".into(), json!(version.as_u128().to_string()));
    object.insert(
        "_deleted".into(),
        json!(if change.op == Op::Delete { 1 } else { 0 }),
    );
    Ok(Value::Object(object))
}

fn tuple_json(column: &str, value: &TupleValue) -> Result<Value> {
    match value {
        TupleValue::Text(text) => Ok(Value::String(text.clone())),
        TupleValue::Null => Ok(Value::Null),
        // An exact-version sink must never turn unavailable source data into
        // NULL: that would make an otherwise valid later LSN query incorrect.
        TupleValue::UnchangedToast => {
            bail!("cannot write exact ClickHouse version: {column} is an unchanged TOAST value")
        }
        TupleValue::Binary(_) => {
            bail!("cannot write exact ClickHouse version: {column} is a binary pgoutput value")
        }
    }
}

async fn clickhouse_request(
    client: &reqwest::Client,
    base_url: &str,
    params: &[(&str, String)],
    body: &str,
) -> Result<(String, reqwest::header::HeaderMap)> {
    let response = client
        .post(base_url)
        .query(params)
        .body(body.to_string())
        .send()
        .await
        .context("ClickHouse request failed")?;
    let status = response.status();
    let headers = response.headers().clone();
    let text = response
        .text()
        .await
        .context("reading ClickHouse response body")?;
    anyhow::ensure!(
        status.is_success(),
        "ClickHouse request failed with status {status}: {}",
        text.lines()
            .next()
            .unwrap_or_default()
            .chars()
            .take(200)
            .collect::<String>()
    );
    Ok((text, headers))
}

/// Renders named parameters, then substitutes the adapter-owned `{target_lsn}`
/// placeholder.
pub fn render_clickhouse_sql(
    sql: &str,
    p: &QueryParameters,
    target_lsn: u64,
) -> Result<String, String> {
    let tenant_set = p
        .tenant_set
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let rendered = sql
        .replace(
            ":window_end",
            &format!("fromUnixTimestamp64Micro({})", p.window_end_micros),
        )
        .replace(":tenant_id", &p.tenant_id.to_string())
        .replace(":tenant_set", &tenant_set);
    if rendered.contains(':') {
        return Err("unresolved named parameter in ClickHouse SQL".into());
    }
    let rendered = rendered.replace("{target_lsn}", &target_lsn.to_string());
    if rendered.contains('{') || rendered.contains('}') {
        return Err("unresolved placeholder in ClickHouse SQL".into());
    }
    Ok(rendered)
}

fn parse_compact_rows(body: &str) -> Result<(Vec<String>, Vec<Vec<Option<String>>>)> {
    let mut lines = body.lines().filter(|line| !line.trim().is_empty());
    let names_line = lines
        .next()
        .context("compact response missing header row")?;
    let names: Vec<String> =
        serde_json::from_str(names_line).context("parsing compact header row")?;
    let _types_line = lines.next().context("compact response missing type row")?;
    let mut rows = Vec::new();
    for line in lines {
        let cells: Vec<Value> = serde_json::from_str(line)
            .with_context(|| format!("parsing compact data row {line}"))?;
        rows.push(cells.into_iter().map(cell_to_string).collect());
    }
    Ok((names, rows))
}

fn cell_to_string(value: Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s),
        other => Some(other.to_string()),
    }
}

fn summary_u64(headers: &reqwest::header::HeaderMap, field: &str) -> Option<u64> {
    let raw = headers.get("X-ClickHouse-Summary")?.to_str().ok()?;
    let summary: Value = serde_json::from_str(raw).ok()?;
    summary
        .get(field)?
        .as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| summary.get(field).and_then(|v| v.as_u64()))
}

fn summary_u128(headers: &reqwest::header::HeaderMap, field: &str) -> Option<u128> {
    let raw = headers.get("X-ClickHouse-Summary")?.to_str().ok()?;
    let summary: Value = serde_json::from_str(raw).ok()?;
    summary
        .get(field)?
        .as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| summary.get(field).and_then(|v| v.as_u64()).map(u128::from))
}

/// Exact-at-LSN query adapter over the ClickHouse HTTP interface.
pub struct ClickHouseAdapter {
    client: reqwest::Client,
    base_url: String,
}

impl ClickHouseAdapter {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into(),
        }
    }

    pub async fn clickhouse_status(&self) -> Result<ClickHouseStatus> {
        let version = self
            .post(&[], "SELECT version() FORMAT TabSeparated")
            .await
            .ok()
            .map(|(body, _)| body.trim().to_string());
        let applied_lsn = self.applied_lsn().await.ok().flatten();
        Ok(ClickHouseStatus {
            healthy: version.is_some(),
            version,
            applied_lsn,
        })
    }

    async fn applied_lsn(&self) -> Result<Option<u64>> {
        let (body, _) = self
            .post(
                &[],
                &format!("SELECT max(source_lsn) FROM {APPLIED_TRANSACTIONS} FORMAT TabSeparated"),
            )
            .await?;
        let raw = body.trim();
        if raw.is_empty() {
            return Ok(None);
        }
        Ok(Some(raw.parse().context("parsing applied LSN")?))
    }

    /// Checkpoint invariant for the physical event stream. Tombstones are part
    /// of the stream: a retry that duplicates one must fail just as a duplicate
    /// live event would.
    pub async fn verify_event_ids_unique(&self) -> Result<()> {
        let (body, _) = self
            .post(
                &[],
                "SELECT count(), uniqExact(event_id) \
                 FROM r1_order_events_raw FORMAT TabSeparated",
            )
            .await?;
        let values: Vec<&str> = body.split_whitespace().collect();
        anyhow::ensure!(
            values.len() == 2,
            "event uniqueness query returned {} fields, expected 2",
            values.len()
        );
        let rows: u64 = values[0]
            .parse()
            .context("parsing physical event row count")?;
        let unique: u64 = values[1]
            .parse()
            .context("parsing unique live event ID count")?;
        anyhow::ensure!(
            rows == unique,
            "duplicate event IDs at checkpoint: count()={rows}, uniqExact(event_id)={unique}"
        );
        Ok(())
    }

    /// Runs an arbitrary SELECT and returns data rows (headers skipped).
    pub async fn select(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        let (body, _) = self
            .post(&[("default_format", COMPACT_FORMAT.to_string())], sql)
            .await?;
        let (_, rows) = parse_compact_rows(&body)?;
        Ok(rows)
    }

    async fn post(
        &self,
        params: &[(&str, String)],
        body: &str,
    ) -> Result<(String, reqwest::header::HeaderMap)> {
        clickhouse_request(&self.client, &self.base_url, params, body).await
    }
}

#[async_trait::async_trait]
impl EngineAdapter for ClickHouseAdapter {
    fn kind(&self) -> EngineKind {
        EngineKind::Clickhouse
    }

    async fn status(&self) -> Result<EngineStatus> {
        let status = self.clickhouse_status().await?;
        Ok(EngineStatus {
            kind: EngineKind::Clickhouse,
            healthy: status.healthy,
            // The sink applies every received batch synchronously, so the
            // receive position and the apply position are the same value.
            received_lsn: status.applied_lsn,
            applied_lsn: status.applied_lsn,
            lag_ms: None,
        })
    }

    async fn wait_visible(&self, target_lsn: u64, timeout: Duration) -> Result<Duration> {
        let start = Instant::now();
        loop {
            if start.elapsed() >= timeout {
                bail!("timeout waiting for LSN {}", target_lsn);
            }
            if let Some(applied) = self.applied_lsn().await.ok().flatten() {
                if applied >= target_lsn {
                    return Ok(start.elapsed());
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn query(&self, invocation: &QueryInvocation) -> Result<QueryResult> {
        let sql_file = match invocation.id {
            QueryId::Q1 => include_str!("../../../bench/r1/queries/clickhouse/q1.sql"),
            QueryId::Q2 => include_str!("../../../bench/r1/queries/clickhouse/q2.sql"),
            QueryId::Q3 => include_str!("../../../bench/r1/queries/clickhouse/q3.sql"),
            QueryId::Q4 => include_str!("../../../bench/r1/queries/clickhouse/q4.sql"),
            QueryId::Q5 => include_str!("../../../bench/r1/queries/clickhouse/q5.sql"),
        };
        let sql = render_clickhouse_sql(sql_file, &invocation.parameters, invocation.target_lsn)
            .map_err(|e| anyhow!("query parameter rendering failed: {e}"))?;

        let applied_before_query = self.applied_lsn().await?.unwrap_or(0);
        anyhow::ensure!(
            applied_before_query >= invocation.target_lsn,
            "LSN proof mismatch: expected {}, got {}",
            invocation.target_lsn,
            applied_before_query
        );
        self.verify_event_ids_unique().await?;

        let (body, headers) = self
            .post(
                &[
                    ("default_format", COMPACT_FORMAT.to_string()),
                    ("send_progress_in_http_headers", "1".to_string()),
                    ("wait_end_of_query", "1".to_string()),
                ],
                &sql,
            )
            .await?;
        let elapsed_ns = summary_u128(&headers, "elapsed_ns")
            .context("ClickHouse response missing elapsed_ns summary")?;

        let (columns, rows) = parse_compact_rows(&body)?;
        // The applied marker proves every transaction through the target LSN is
        // ingested; the SQL is exact at the target by its _source_lsn reduction.
        // A live source can commit past the target, so the proof is `>=`, and
        // the result is reported at the target LSN it was constructed for.
        let applied = self.applied_lsn().await?.unwrap_or(0);
        anyhow::ensure!(
            applied >= invocation.target_lsn,
            "LSN proof mismatch: expected {}, got {}",
            invocation.target_lsn,
            applied
        );
        Ok(QueryResult {
            columns,
            rows,
            target_lsn: invocation.target_lsn,
            visible_lsn: invocation.target_lsn,
            elapsed_ns,
            rows_read: summary_u64(&headers, "read_rows"),
            bytes_read: summary_u64(&headers, "read_bytes"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::LogicalCheckpoint;
    use bytes::{BufMut, Bytes};
    use graydb_log::Frame;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    const MARKER_SELECT: &str = "SELECT operation_sha256 FROM r1_meta.applied_transactions";
    const MARKER_INSERT: &str = "INSERT INTO r1_meta.applied_transactions";
    const ORDERS_INSERT: &str = "INSERT INTO r1_orders_raw";
    const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    struct MarkerLookupSequence {
        calls: Arc<AtomicUsize>,
    }

    #[derive(Default)]
    struct RecordingAcknowledger {
        lsns: Vec<u64>,
    }

    #[async_trait::async_trait]
    impl ReplicationAcknowledger for RecordingAcknowledger {
        async fn acknowledge_applied(&mut self, lsn: u64) -> Result<()> {
            self.lsns.push(lsn);
            Ok(())
        }
    }

    impl Respond for MarkerLookupSequence {
        fn respond(&self, _request: &Request) -> ResponseTemplate {
            let body = if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                ""
            } else {
                HASH_B
            };
            ResponseTemplate::new(200).set_body_string(body)
        }
    }

    /// Test fixture mirroring the production version-reduction rule (spec
    /// section 10): among versions with `_source_lsn <= target`, pick the
    /// greatest `_version`; a tombstone there makes the key invisible.
    struct VersionedFixtureRow {
        key: u64,
        source_lsn: u64,
        version: Version,
        deleted: bool,
        status: Option<&'static str>,
    }

    fn select_visible_fixture<'a>(
        rows: &'a [VersionedFixtureRow],
        key: u64,
        target_lsn: u64,
    ) -> Option<&'a VersionedFixtureRow> {
        rows.iter()
            .filter(|row| row.key == key && row.source_lsn <= target_lsn)
            .max_by_key(|row| row.version)
            .filter(|row| !row.deleted)
    }

    fn order_image(order_id: u64, status: &str, amount: i64) -> Vec<(String, TupleValue)> {
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
    }

    fn order_change(op: Op, order_id: u64, status: &str, amount: i64) -> TypedChange {
        let new =
            (op == Op::Insert || op == Op::Update).then(|| order_image(order_id, status, amount));
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

    fn frame(seq: u64, lsn: u64, payload: Vec<u8>) -> Frame {
        Frame {
            seq,
            lsn_start: lsn,
            lsn_end: lsn,
            txn_complete: false,
            payload: Bytes::from(payload),
        }
    }

    fn begin(xid: u32) -> Vec<u8> {
        let mut bytes = vec![b'B'];
        bytes.put_u64(0);
        bytes.put_i64(0);
        bytes.put_u32(xid);
        bytes
    }

    fn commit(end_lsn: u64) -> Vec<u8> {
        let mut bytes = vec![b'C'];
        bytes.put_u8(0);
        bytes.put_u64(end_lsn - 1);
        bytes.put_u64(end_lsn);
        bytes.put_i64(0);
        bytes
    }

    fn orders_relation() -> Vec<u8> {
        let fields = [
            "order_id",
            "tenant_id",
            "customer_id",
            "status",
            "channel",
            "amount_cents",
            "created_at",
            "updated_at",
            "attributes",
        ];
        let mut bytes = vec![b'R'];
        bytes.put_u32(42);
        bytes.extend(b"r1\0orders\0");
        bytes.put_u8(b'd');
        bytes.put_u16(fields.len() as u16);
        for field in fields {
            bytes.put_u8(0);
            bytes.extend(field.as_bytes());
            bytes.put_u8(0);
            bytes.put_u32(25);
            bytes.put_i32(-1);
        }
        bytes
    }

    fn order_insert(values: &[&str]) -> Vec<u8> {
        let mut bytes = vec![b'I'];
        bytes.put_u32(42);
        bytes.put_u8(b'N');
        bytes.put_u16(values.len() as u16);
        for value in values {
            bytes.put_u8(b't');
            bytes.put_u32(value.len() as u32);
            bytes.extend(value.as_bytes());
        }
        bytes
    }

    #[test]
    fn raw_ddl_keeps_every_historical_version_as_a_distinct_sort_key() {
        let ddl = include_str!("../../../bench/r1/clickhouse.sql");
        assert!(!ddl.contains("ReplacingMergeTree"), "{ddl}");
        for key in ["tenant_id", "customer_id", "order_id", "event_id"] {
            assert!(
                ddl.contains(&format!(
                    "ORDER BY ({key}, _version)\nSETTINGS non_replicated_deduplication_window = 1000000;"
                )),
                "missing immutable sort key for {key}:\n{ddl}"
            );
        }
    }

    #[tokio::test]
    async fn unsupported_published_table_is_a_hard_error() {
        let sink = ClickHouseSink::new("http://127.0.0.1:1");
        let mut change = order_change(Op::Insert, 1, "paid", 100);
        change.table = "r1.unexpected".into();
        let error = sink
            .insert_version_rows(500, HASH_A, &[change], false)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("unsupported published table"),
            "error: {error:#}"
        );
    }

    #[test]
    fn version_orders_changes_inside_and_across_commits() {
        let a = Version::from_lsn_ordinal(100, 1);
        let b = Version::from_lsn_ordinal(100, 2);
        let c = Version::from_lsn_ordinal(101, 0);
        assert!(a < b);
        assert!(b < c);
        assert_eq!(c.as_u128(), 101u128 << 32);
    }

    #[test]
    fn visible_version_excludes_tombstone_at_target_lsn() {
        let rows = vec![
            VersionedFixtureRow {
                key: 1,
                source_lsn: 100,
                version: Version::from_lsn_ordinal(100, 1),
                deleted: false,
                status: Some("new"),
            },
            VersionedFixtureRow {
                key: 1,
                source_lsn: 200,
                version: Version::from_lsn_ordinal(200, 1),
                deleted: false,
                status: Some("paid"),
            },
            VersionedFixtureRow {
                key: 1,
                source_lsn: 300,
                version: Version::from_lsn_ordinal(300, 1),
                deleted: true,
                status: None,
            },
        ];
        assert_eq!(
            select_visible_fixture(&rows, 1, 250).unwrap().status,
            Some("paid")
        );
        assert_eq!(
            select_visible_fixture(&rows, 1, 150).unwrap().status,
            Some("new")
        );
        assert!(select_visible_fixture(&rows, 1, 300).is_none());
        assert!(select_visible_fixture(&rows, 1, 99).is_none());
    }

    #[test]
    fn version_row_rejects_unchanged_toast_instead_of_writing_null() {
        let mut change = order_change(Op::Update, 1, "paid", 100);
        let image = change.new.as_mut().expect("update has a new tuple");
        let (_, attributes) = image
            .iter_mut()
            .find(|(column, _)| column == "attributes")
            .expect("order image has attributes");
        *attributes = TupleValue::UnchangedToast;

        let error = render_row(
            raw_table_for("r1.orders").expect("orders is published"),
            &change,
            500,
            1,
            Version::from_lsn_ordinal(500, 1),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("unchanged TOAST"),
            "error: {error:#}"
        );
    }

    #[tokio::test]
    async fn retried_transaction_with_matching_marker_skips_re_insert() {
        let server = MockServer::start().await;

        // Marker lookups: the first sees no marker, later ones see the applied one.
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(MARKER_SELECT))
            .respond_with(ResponseTemplate::new(200).set_body_string(""))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(MARKER_SELECT))
            .respond_with(ResponseTemplate::new(200).set_body_string(HASH_A))
            .mount(&server)
            .await;

        // Data inserts: the first succeeds, any later one fails loudly so a
        // non-idempotent retry cannot pass silently.
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(ORDERS_INSERT))
            .respond_with(ResponseTemplate::new(200).set_body_string(""))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(ORDERS_INSERT))
            .respond_with(ResponseTemplate::new(500).set_body_string("duplicate insert"))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(MARKER_INSERT))
            .respond_with(ResponseTemplate::new(200).set_body_string(""))
            .mount(&server)
            .await;

        let sink = ClickHouseSink::new(server.uri());
        let changes = vec![order_change(Op::Insert, 1, "paid", 100)];
        assert_eq!(
            sink.apply_transaction(500, HASH_A, &changes).await.unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(
            sink.apply_transaction(500, HASH_A, &changes).await.unwrap(),
            ApplyOutcome::SkippedIdempotent
        );
    }

    #[tokio::test]
    async fn mismatched_marker_hash_at_same_lsn_is_a_hard_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(MARKER_SELECT))
            .respond_with(ResponseTemplate::new(200).set_body_string(HASH_B))
            .mount(&server)
            .await;

        let sink = ClickHouseSink::new(server.uri());
        let err = sink
            .apply_transaction(500, HASH_A, &[order_change(Op::Insert, 1, "paid", 100)])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("idempotency marker hash mismatch"),
            "error: {err}"
        );
    }

    #[tokio::test]
    async fn apply_requires_a_verified_marker_before_reporting_success() {
        let server = MockServer::start().await;
        let lookup_count = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(MARKER_SELECT))
            .respond_with(MarkerLookupSequence {
                calls: Arc::clone(&lookup_count),
            })
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(ORDERS_INSERT))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(MARKER_INSERT))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let sink = ClickHouseSink::new(server.uri());
        let mut acknowledger = RecordingAcknowledger::default();
        let error = sink
            .apply(
                &mut acknowledger,
                500,
                HASH_A,
                &[order_change(Op::Insert, 1, "paid", 100)],
            )
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("idempotency marker hash mismatch"),
            "error: {error:#}"
        );
        assert_eq!(lookup_count.load(Ordering::SeqCst), 2);
        assert!(
            acknowledger.lsns.is_empty(),
            "PostgreSQL must not be acknowledged before marker verification"
        );
    }

    #[tokio::test]
    async fn duplicate_transaction_markers_are_a_hard_correctness_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(MARKER_SELECT))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(format!("{HASH_A}\n{HASH_A}\n")),
            )
            .mount(&server)
            .await;

        let sink = ClickHouseSink::new(server.uri());
        let error = sink
            .apply_transaction(500, HASH_A, &[order_change(Op::Insert, 1, "paid", 100)])
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("duplicate transaction markers"),
            "error: {error:#}"
        );
    }

    #[tokio::test]
    async fn execute_sends_each_ddl_statement_as_its_own_request() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        ClickHouseSink::new(server.uri())
            .execute("CREATE DATABASE r1_meta; CREATE TABLE marker (id UInt64);")
            .await
            .unwrap();

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 2, "one HTTP query per DDL statement");
        assert_eq!(
            String::from_utf8(requests[0].body.clone()).unwrap(),
            "CREATE DATABASE r1_meta"
        );
        assert_eq!(
            String::from_utf8(requests[1].body.clone()).unwrap(),
            "CREATE TABLE marker (id UInt64)"
        );
    }

    #[test]
    fn exact_sql_rendering_contains_version_reduction_with_target_lsn() {
        let params = QueryParameters {
            window_end_micros: 1_700_000_000_000_000,
            tenant_id: 5,
            tenant_set: vec![5, 9],
        };
        let sql = render_clickhouse_sql(
            include_str!("../../../bench/r1/queries/clickhouse/q1.sql"),
            &params,
            12345,
        )
        .unwrap();
        assert!(sql.contains("_source_lsn <= 12345"), "{sql}");
        assert!(sql.contains("argMax("), "{sql}");
        assert!(
            sql.contains("fromUnixTimestamp64Micro(1700000000000000)"),
            "{sql}"
        );
        assert!(!sql.contains("{target_lsn}"), "{sql}");
    }

    fn invocation(target_lsn: u64) -> QueryInvocation {
        QueryInvocation {
            id: QueryId::Q5,
            parameters: QueryParameters {
                window_end_micros: 1_700_000_000_000_000,
                tenant_id: 5,
                tenant_set: vec![5],
            },
            checkpoint: LogicalCheckpoint {
                sequence: 1,
                source_lsn: target_lsn,
            },
            target_lsn,
        }
    }

    async fn mount_query_and_applied(server: &MockServer, applied_lsn: &str) {
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains("argMax("))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(
                        "X-ClickHouse-Summary",
                        r#"{"read_rows":"7","read_bytes":"42","elapsed_ns":"123456"}"#,
                    )
                    .set_body_string(
                        "[\"status\",\"count(*)\"]\n[\"String\",\"UInt64\"]\n[\"paid\",\"2\"]\n",
                    ),
            )
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains("uniqExact(event_id)"))
            .respond_with(ResponseTemplate::new(200).set_body_string("3\t3\n"))
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(
                "max(source_lsn) FROM r1_meta.applied_transactions",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string(applied_lsn.to_string()))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn adapter_query_parses_compact_rows_and_captures_summary_headers() {
        let server = MockServer::start().await;
        mount_query_and_applied(&server, "500").await;

        let adapter = ClickHouseAdapter::new(server.uri());
        let result = adapter.query(&invocation(500)).await.unwrap();
        assert_eq!(result.columns, vec!["status", "count(*)"]);
        assert_eq!(
            result.rows,
            vec![vec![Some("paid".into()), Some("2".into())]]
        );
        assert_eq!(result.visible_lsn, 500);
        assert_eq!(result.rows_read, Some(7));
        assert_eq!(result.bytes_read, Some(42));
        assert_eq!(result.elapsed_ns, 123_456);
    }

    #[tokio::test]
    async fn adapter_query_rejects_stale_applied_lsn_with_proof_mismatch() {
        let server = MockServer::start().await;
        mount_query_and_applied(&server, "499").await;

        let adapter = ClickHouseAdapter::new(server.uri());
        let err = adapter.query(&invocation(500)).await.unwrap_err();
        assert!(
            err.to_string().contains("LSN proof mismatch"),
            "error: {err}"
        );
    }

    #[tokio::test]
    async fn adapter_checks_visibility_before_running_the_exact_query() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(
                "max(source_lsn) FROM r1_meta.applied_transactions",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string("499"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains("argMax("))
            .respond_with(
                ResponseTemplate::new(500).set_body_string("exact query must not run while stale"),
            )
            .mount(&server)
            .await;

        let adapter = ClickHouseAdapter::new(server.uri());
        let error = adapter.query(&invocation(500)).await.unwrap_err();
        assert!(
            error.to_string().contains("LSN proof mismatch"),
            "error: {error:#}"
        );
        let query_requests = server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .filter(|request| String::from_utf8_lossy(&request.body).contains("argMax("))
            .count();
        assert_eq!(query_requests, 0, "stale state must not be queried");
    }

    #[tokio::test]
    async fn adapter_rejects_duplicate_event_ids_before_checkpoint_query() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(
                "max(source_lsn) FROM r1_meta.applied_transactions",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string("500"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains("uniqExact(event_id)"))
            .respond_with(ResponseTemplate::new(200).set_body_string("4\t3\n"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains("argMax("))
            .respond_with(
                ResponseTemplate::new(500).set_body_string("query must not run after duplicate"),
            )
            .mount(&server)
            .await;

        let adapter = ClickHouseAdapter::new(server.uri());
        let error = adapter.query(&invocation(500)).await.unwrap_err();
        assert!(
            error.to_string().contains("duplicate event IDs"),
            "error: {error:#}"
        );
    }

    #[tokio::test]
    async fn adapter_rejects_duplicate_tombstone_event_ids_in_the_physical_stream() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(
                "max(source_lsn) FROM r1_meta.applied_transactions",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string("500"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains("uniqExact(event_id)"))
            .respond_with(ResponseTemplate::new(200).set_body_string("4\t3\n"))
            .mount(&server)
            .await;

        let adapter = ClickHouseAdapter::new(server.uri());
        let error = adapter.query(&invocation(500)).await.unwrap_err();
        assert!(
            error.to_string().contains("duplicate event IDs"),
            "error: {error:#}"
        );
        let duplicate_check = server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .find(|request| String::from_utf8_lossy(&request.body).contains("uniqExact(event_id)"))
            .expect("physical duplicate check request");
        assert!(
            !String::from_utf8_lossy(&duplicate_check.body).contains("_deleted = 0"),
            "tombstones must participate in the physical duplicate invariant"
        );
    }

    #[tokio::test]
    async fn decoded_frames_apply_only_after_commit_in_original_order() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(MARKER_SELECT))
            .respond_with(ResponseTemplate::new(200).set_body_string(""))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(MARKER_SELECT))
            .respond_with(ResponseTemplate::new(200).set_body_string(HASH_A))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(ORDERS_INSERT))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_string_contains(MARKER_INSERT))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let frames = vec![
            frame(0, 10, orders_relation()),
            frame(1, 11, begin(77)),
            frame(
                2,
                12,
                order_insert(&[
                    "1",
                    "101",
                    "102",
                    "paid",
                    "web",
                    "100",
                    "2026-09-01 00:00:00",
                    "2026-09-01 00:00:01",
                    "{}",
                ]),
            ),
            frame(
                3,
                13,
                order_insert(&[
                    "2",
                    "101",
                    "102",
                    "shipped",
                    "web",
                    "200",
                    "2026-09-01 00:00:00",
                    "2026-09-01 00:00:01",
                    "{}",
                ]),
            ),
            frame(4, 500, commit(500)),
        ];
        let mut cdc = ClickHouseCdcAdapter::new(server.uri());
        let mut acknowledger = RecordingAcknowledger::default();
        assert!(cdc
            .apply_frames(&mut acknowledger, HASH_A, &frames[..4])
            .await
            .unwrap()
            .is_none());
        assert!(server.received_requests().await.unwrap().is_empty());

        assert_eq!(
            cdc.apply_frames(&mut acknowledger, HASH_A, &frames[4..])
                .await
                .unwrap(),
            Some(ApplyOutcome::Applied)
        );
        assert_eq!(acknowledger.lsns, vec![500]);
        let data_insert = server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .find(|request| String::from_utf8_lossy(&request.body).contains(ORDERS_INSERT))
            .expect("order batch inserted after commit");
        let body = String::from_utf8(data_insert.body).unwrap();
        assert!(body.contains("\"order_id\":\"1\""), "{body}");
        assert!(
            body.contains("\"change_ordinal\":1") || body.contains("\"_change_ordinal\":1"),
            "{body}"
        );
        assert!(body.contains("\"_change_ordinal\":2"), "{body}");
        assert!(
            body.find("\"order_id\":\"1\"").unwrap() < body.find("\"order_id\":\"2\"").unwrap(),
            "{body}"
        );
    }
}
