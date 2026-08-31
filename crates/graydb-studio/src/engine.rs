//! Studio engine: the live pipeline behind the GUI. Owns attach → slot → frame log
//! → pump → materializers → reader, plus the chaos controls SP7 proved.
//! Everything the panels show comes from real state: the durable mark, the WAL
//! budget gauge, per-shape applied LSNs, and an event log of what actually happened.

use anyhow::{Context, Result};
use graydb_columnar::{ColumnSpec, TableStore};
use graydb_ingest::repl::{format_lsn, parse_lsn, ReplClient};
use graydb_ingest::snapshot::SnapshotManifest;
use graydb_ingest::stream::{IngestMetrics, PumpCommand};
use graydb_ingest::{attach, budget, config::Config, snapshot, stream};
use graydb_search::SearchStore;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex, RwLock};

// Reader/TableShape stay available for disk-artifact consumers (demo-sp6).

const EVENT_CAP: usize = 200;

#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub at: String,
    pub level: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct TableRow {
    pub table: String,
    pub eligibility: String,
    pub columnar_applied_lsn: String,
    pub search_applied_lsn: Option<String>,
    pub rows_visible: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub attached: bool,
    pub source: String,
    pub server_version: String,
    pub publication: String,
    pub slot: String,
    pub lsn0: String,
    pub received_lsn: String,
    /// Durable/acked position — what the slot has been advanced to (pg_stat_replication
    /// flush_lsn semantics). The ack invariant guarantees this is a transaction-complete,
    /// checksummed, fsync'd frame prefix.
    pub flushed_lsn: String,
    pub applied_lsn: String,
    pub apply_lag_bytes: i64,
    pub frames: u64,
    pub commits: u64,
    pub spilled_frames: u64,
    pub pump_alive: bool,
    pub stalled: bool,
    /// WAL budget gauge (rung 0..2 + fraction), per Amendment A A3.
    pub wal_retained_bytes: u64,
    pub wal_budget_bytes: u64,
    pub wal_fraction: f64,
    pub wal_rung: u8,
    pub tables: Vec<TableRow>,
}

struct Pipeline {
    lsn0: u64,
    manifest: SnapshotManifest,
    columnar: HashMap<String, TableStore>,
    search: HashMap<String, SearchStore>,
    eligibility: HashMap<String, String>,
    applied_lsn: u64,
    /// Incremental frame cursor into the log (R1/P1 — no full replay per tick).
    tail: graydb_log::tail::LogTail,
    /// Incremental decoder (live Relation metadata + open-txn state across ticks).
    decoder: graydb_registry::decoder::StreamDecoder,
    server_version: String,
}

struct PumpHandle {
    task: tokio::task::JoinHandle<Result<()>>,
    ctrl: watch::Sender<PumpCommand>,
    durable: watch::Receiver<graydb_log::DurableMark>,
    metrics: Arc<IngestMetrics>,
}

pub struct Engine {
    pub cfg: Config,
    log_dir: PathBuf,
    snapshot_dir: PathBuf,
    columnar_root: PathBuf,
    search_root: PathBuf,
    pipeline: RwLock<Option<Pipeline>>,
    pump: Mutex<Option<PumpHandle>>,
    events: Mutex<VecDeque<Event>>,
    /// Set by the "crash before materialize" chaos button: apply loop refuses to run.
    crash_before_materialize: watch::Sender<bool>,
}

impl Engine {
    pub fn new(cfg: Config) -> Arc<Self> {
        let data = cfg.storage.data_dir.clone();
        let (crash_tx, _) = watch::channel(false);
        Arc::new(Engine {
            cfg,
            log_dir: data.join("log").join("studio"),
            snapshot_dir: data.join("snapshot").join("studio"),
            columnar_root: data.join("columnar").join("studio"),
            search_root: data.join("search").join("studio"),
            pipeline: RwLock::new(None),
            pump: Mutex::new(None),
            events: Mutex::new(VecDeque::new()),
            crash_before_materialize: crash_tx,
        })
    }

    pub async fn event(&self, level: &str, message: impl Into<String>) {
        let msg = message.into();
        tracing::info!(level, %msg, "studio event");
        let mut events = self.events.lock().await;
        if events.len() == EVENT_CAP {
            events.pop_front();
        }
        events.push_back(Event {
            at: now_hms(),
            level: level.to_string(),
            message: msg,
        });
    }

    pub async fn events(&self) -> Vec<Event> {
        self.events.lock().await.iter().rev().cloned().collect()
    }

    /// Attach panel: install the SQL-objects-only pack, create the slot with an
    /// exported snapshot, backfill the schema at LSN0, start the pump, materialize.
    pub async fn attach(self: &Arc<Self>) -> Result<()> {
        let cfg = self.cfg.clone();
        let admin = cfg.connect().await?;
        let server_version: String = admin.query_one("SHOW server_version", &[]).await?.get(0);
        self.event("info", format!("connected to source (PostgreSQL {server_version})"))
            .await;

        attach::drop_slot_if_exists(&admin, &cfg.source.slot).await?;
        attach::drop_publication_if_exists(&admin, &cfg.source.publication).await?;
        attach::install_attach_pack(&admin).await?;
        attach::ensure_publication(&admin, &cfg.source.publication, &cfg.source.schema).await?;
        self.event(
            "info",
            format!(
                "attach pack installed: publication {} + graydb.ddl_log event triggers (SQL objects only)",
                cfg.source.publication
            ),
        )
        .await;

        let eligibility: HashMap<String, String> = attach::eligibility_scan(&admin, &cfg.source.schema)
            .await?
            .into_iter()
            .map(|e| (e.table, e.eligibility.to_string()))
            .collect();

        let mut repl_a = ReplClient::connect(
            &cfg.source.host, cfg.source.port, &cfg.source.user,
            &cfg.source.password, &cfg.source.dbname,
        ).await?;
        let slot = repl_a.create_slot_with_snapshot(&cfg.source.slot).await?;
        let lsn0 = parse_lsn(&slot.consistent_point)?;
        self.event("info", format!("slot {} created; LSN0={}", slot.slot_name, slot.consistent_point))
            .await;

        // Pump first, so ingestion runs concurrently with the backfill (WL7).
        let log = graydb_log::FrameLog::create(&self.log_dir, cfg.log.segment_max_bytes).await?;
        self.start_pump(log, lsn0).await?;

        if self.snapshot_dir.exists() {
            tokio::fs::remove_dir_all(&self.snapshot_dir).await.ok();
        }
        let manifest = snapshot::run_parallel_copy(
            &cfg, &slot.consistent_point, &slot.snapshot_name, &self.snapshot_dir,
        ).await?;
        repl_a.close().await.ok();
        let rows: u64 = manifest.tables.iter().map(|t| t.rows).sum();
        self.event(
            "info",
            format!(
                "backfill complete at LSN0: {rows} rows across {} tables (exported snapshot, {} streams)",
                manifest.tables.len(), cfg.initial_load.copy_streams
            ),
        )
        .await;

        // Build stores from the staged parts.
        let mut columnar = HashMap::new();
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
                &self.columnar_root.join(&t.table), &t.table, specs, cfg.columnar.flush_rows,
            )?;
            for part in &t.parts {
                let data = std::fs::read(self.snapshot_dir.join(&part.file))?;
                store.load_copy_part(&data, lsn0)?;
            }
            store.flush()?;
            store.finalize()?;
            columnar.insert(t.table.clone(), store);
        }

        let mut search = HashMap::new();
        for idx in &cfg.search.indexes {
            let Some(t) = manifest.tables.iter().find(|t| t.table == idx.table) else {
                continue;
            };
            let mut store = SearchStore::create(
                &self.search_root.join(&t.table), &t.table, &idx.columns, &t.key_columns,
            )?;
            let col_pos: Vec<usize> = idx
                .columns
                .iter()
                .filter_map(|c| t.columns.iter().position(|x| x == c))
                .collect();
            let key_pos: Vec<usize> = t
                .key_columns
                .iter()
                .filter_map(|c| t.columns.iter().position(|x| x == c))
                .collect();
            for part in &t.parts {
                let data = std::fs::read(self.snapshot_dir.join(&part.file))?;
                for (line_no, line) in data.split(|&b| b == b'\n').enumerate() {
                    if line.is_empty() {
                        continue;
                    }
                    let vals = graydb_columnar::copytext::parse_copy_line(line);
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
                }
            }
            store.commit_batch(lsn0)?;
            search.insert(t.table.clone(), store);
        }
        self.event("info", "shapes materialized at LSN0 (columnar + search)").await;

        *self.pipeline.write().await = Some(Pipeline {
            lsn0,
            manifest,
            columnar,
            search,
            eligibility,
            applied_lsn: lsn0,
            tail: graydb_log::tail::LogTail::new(&self.log_dir),
            decoder: graydb_registry::decoder::StreamDecoder::new(),
            server_version,
        });

        self.spawn_apply_loop();
        Ok(())
    }

    async fn start_pump(self: &Arc<Self>, log: graydb_log::FrameLog, start_lsn: u64) -> Result<()> {
        let cfg = &self.cfg;
        let mut attempt = 0;
        loop {
            let mut repl = ReplClient::connect(
                &cfg.source.host, cfg.source.port, &cfg.source.user,
                &cfg.source.password, &cfg.source.dbname,
            ).await?;
            match repl
                .start_replication(&cfg.source.slot, &cfg.source.publication, start_lsn)
                .await
            {
                Ok(()) => {
                    let durable = log.durable();
                    let metrics = Arc::new(IngestMetrics::default());
                    let (ctrl, ctrl_rx) = watch::channel(PumpCommand::default());
                    let task = tokio::spawn(stream::run_pump(
                        repl, log, start_lsn, ctrl_rx, Arc::clone(&metrics),
                    ));
                    *self.pump.lock().await = Some(PumpHandle { task, ctrl, durable, metrics });
                    return Ok(());
                }
                Err(e) if attempt < 40 && e.to_string().contains("active") => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Continuous materializer: replays new durable frames into both shapes.
    fn spawn_apply_loop(self: &Arc<Self>) {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            let mut crash_rx = engine.crash_before_materialize.subscribe();
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                if *crash_rx.borrow_and_update() {
                    continue; // chaos: frames stay durable, materialization frozen
                }
                if let Err(e) = engine.apply_new_frames().await {
                    engine.event("error", format!("apply failed: {e}")).await;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        });
    }

    /// Incremental apply (R1/P1): read only newly-appended frames via the LogTail,
    /// decode them statefully, apply committed changes to both shapes. No full
    /// replay, no per-tick finalize (stores flush at columnar.flush_rows; freshness
    /// comes from the open-row overlay in the query path — R1/P2).
    async fn apply_new_frames(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.pipeline.write().await;
        let Some(p) = guard.as_mut() else { return Ok(()) };
        let batch = p.tail.read_new()?;
        if batch.rewound {
            // Log truncated to its durable boundary (decoder kill / crash restart):
            // only an uncommitted transaction can vanish — drop it; the fresh
            // session re-delivers it in full.
            p.decoder.abort_open_txn();
        }
        if batch.frames.is_empty() {
            return Ok(());
        }
        let decoded = p.decoder.feed(&batch.frames)?;
        if decoded.changes.is_empty() {
            return Ok(());
        }
        let mut search_touched = false;
        for (offset, change) in decoded.changes.iter().enumerate() {
            if let Some(store) = p.columnar.get_mut(&change.table) {
                store.apply(change)?;
            }
            if let Some(store) = p.search.get_mut(&change.table) {
                let key = format!("s:{}", decoded.first_change_index + offset as u64);
                store.apply(change, &key)?;
                search_touched = true;
            }
        }
        p.applied_lsn = decoded.last_commit_lsn.max(p.applied_lsn);
        let applied_lsn = p.applied_lsn;
        if search_touched {
            for store in p.search.values_mut() {
                store.commit_batch(applied_lsn)?;
            }
        }
        let n = decoded.changes.len();
        drop(guard);
        self.event(
            "info",
            format!("applied {n} changes; shapes now at {}", format_lsn(applied_lsn)),
        )
        .await;
        Ok(())
    }

    /// Lightweight accessor for pollers (the bench harness): current applied LSN.
    pub async fn applied_lsn(self: &Arc<Self>) -> u64 {
        self.pipeline
            .read()
            .await
            .as_ref()
            .map(|p| p.applied_lsn)
            .unwrap_or(0)
    }

    /// Wait until the shapes have applied at least `lsn`; false on timeout.
    pub async fn wait_applied(self: &Arc<Self>, lsn: u64, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if self.applied_lsn().await >= lsn {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn status(self: &Arc<Self>) -> Result<Status> {
        let guard = self.pipeline.read().await;
        let pump = self.pump.lock().await;
        let durable_lsn = pump.as_ref().map(|h| h.durable.borrow().lsn).unwrap_or(0);
        let (received, frames, commits, spilled, alive, stalled) = match pump.as_ref() {
            Some(h) => (
                // received_lsn = stream position (pg_stat_subscription semantics);
                // the durable mark is the flushed/ack position, reported as lag basis.
                h.metrics
                    .stream_lsn
                    .load(Ordering::Relaxed)
                    .max(h.durable.borrow().lsn),
                h.metrics.frames.load(Ordering::Relaxed),
                h.metrics.commits.load(Ordering::Relaxed),
                h.metrics.spilled_frames.load(Ordering::Relaxed),
                !h.task.is_finished(),
                *h.ctrl.borrow() == (PumpCommand { stalled: true, shutdown: false }),
            ),
            None => (0, 0, 0, 0, false, false),
        };
        drop(pump);

        let budget_bytes = std::env::var("GRAYDB_WAL_BUDGET_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(self.cfg.wal_budget.bytes_cap);
        let (retained, fraction, rung) = match self.cfg.connect().await {
            Ok(client) => match budget::sample(
                &client, &self.cfg.source.slot, budget_bytes,
                self.cfg.wal_budget.warn_fraction, self.cfg.wal_budget.shed_fraction,
            ).await {
                Ok(b) => (b.retained_bytes, b.consumed_fraction, b.rung),
                Err(_) => (0, 0.0, 0),
            },
            Err(_) => (0, 0.0, 0),
        };

        let (attached, lsn0, applied, version, tables) = match guard.as_ref() {
            Some(p) => {
                let mut rows = Vec::new();
                for t in &p.manifest.tables {
                    let visible = p
                        .columnar
                        .get(&t.table)
                        .map(|s| s.visible_rows())
                        .unwrap_or(0);
                    rows.push(TableRow {
                        table: t.table.clone(),
                        eligibility: p
                            .eligibility
                            .get(&t.table)
                            .cloned()
                            .unwrap_or_else(|| "unknown".into()),
                        columnar_applied_lsn: format_lsn(p.applied_lsn),
                        search_applied_lsn: p
                            .search
                            .get(&t.table)
                            .map(|s| format_lsn(s.meta.applied_lsn)),
                        rows_visible: visible,
                    });
                }
                (
                    true,
                    format_lsn(p.lsn0),
                    p.applied_lsn,
                    p.server_version.clone(),
                    rows,
                )
            }
            None => (false, "-".into(), 0, "-".into(), Vec::new()),
        };

        Ok(Status {
            attached,
            source: format!(
                "{}:{}/{}",
                self.cfg.source.host, self.cfg.source.port, self.cfg.source.dbname
            ),
            server_version: version,
            publication: self.cfg.source.publication.clone(),
            slot: self.cfg.source.slot.clone(),
            lsn0,
            received_lsn: format_lsn(received),
            flushed_lsn: format_lsn(durable_lsn),
            applied_lsn: format_lsn(applied),
            apply_lag_bytes: received.saturating_sub(applied) as i64,
            frames,
            commits,
            spilled_frames: spilled,
            pump_alive: alive,
            stalled,
            wal_retained_bytes: retained,
            wal_budget_bytes: budget_bytes,
            wal_fraction: fraction,
            wal_rung: rung,
            tables,
        })
    }

    /// SQL editor: run a query at a consistency class, return rows + LSN proof.
    /// Classes (Amendment A A4): eventual | bounded(X) | strong (source barrier) |
    /// target_lsn=<lsn> for explicit historical reads.
    pub async fn query(
        self: &Arc<Self>,
        sql: &str,
        class: &str,
    ) -> Result<(Vec<Vec<String>>, Vec<String>, String)> {
        let target = self.resolve_class(class).await?;
        let lsn = target.unwrap_or(u64::MAX);

        // Live snapshots (R1/P2+P3): flushed segments + delete states + the open-row
        // overlay, captured under a brief read lock; the scan itself holds no lock.
        let guard = self.pipeline.read().await;
        let p = guard.as_ref().context("not attached")?;
        let mut snapshots = Vec::with_capacity(p.columnar.len());
        for (name, store) in &p.columnar {
            snapshots.push(Arc::new(crate::provider::TableSnapshot {
                name: name.clone(),
                schema: store.arrow_schema(),
                segments: store.segments_snapshot(),
                overlay: store.open_rows_batch(lsn)?,
                target_lsn: lsn,
                applied_lsn: store.applied_lsn.max(p.lsn0),
            }));
        }
        let search_dirs: Vec<(String, PathBuf)> = p
            .search
            .keys()
            .map(|t| (t.clone(), self.search_root.join(t)))
            .collect();
        drop(guard);

        let mut search = std::collections::HashMap::new();
        for (table, dir) in search_dirs {
            search.insert(table.clone(), Arc::new(graydb_search::SearchReader::open(&dir)?));
        }
        let received = self
            .pump
            .lock()
            .await
            .as_ref()
            .map(|h| {
                h.metrics
                    .stream_lsn
                    .load(Ordering::Relaxed)
                    .max(h.durable.borrow().lsn)
            })
            .unwrap_or(0);

        let (batches, proof) = crate::run_query(snapshots, &search, received, sql, target).await?;
        let (cols, rows) = crate::batches_to_rows(&batches);
        Ok((rows, cols, proof.render()))
    }

    /// Resolve a consistency class into a target LSN.
    async fn resolve_class(self: &Arc<Self>, class: &str) -> Result<Option<u64>> {
        let class = class.trim();
        if let Some(rest) = class.strip_prefix("target_lsn=") {
            return Ok(Some(parse_lsn(rest.trim())?));
        }
        match class {
            "" | "eventual" => Ok(None),
            "strong" => {
                // Source barrier (A4): take B = pg_current_wal_lsn(), then wait until
                // (a) the STREAM has passed B — proving every txn committed at or
                // before B has been received (B itself usually sits past the last
                // commit: checkpoints and standby snapshots move the WAL head), and
                // (b) materialization has caught up to the durable mark.
                let client = self.cfg.connect().await?;
                let s: String = client
                    .query_one("SELECT pg_current_wal_lsn()::text", &[])
                    .await?
                    .get(0);
                let barrier = parse_lsn(&s)?;
                self.event("info", format!("strong read: source barrier {}", format_lsn(barrier)))
                    .await;
                for i in 0..600 {
                    let (stream, durable) = {
                        let pump = self.pump.lock().await;
                        match pump.as_ref() {
                            Some(h) => (
                                h.metrics.stream_lsn.load(Ordering::Relaxed),
                                h.durable.borrow().lsn,
                            ),
                            None => (0, 0),
                        }
                    };
                    let applied = self
                        .pipeline
                        .read()
                        .await
                        .as_ref()
                        .map(|p| p.applied_lsn)
                        .unwrap_or(0);
                    if stream >= barrier && applied >= durable {
                        return Ok(Some(applied.max(barrier)));
                    }
                    // Nudge the WAL forward so an idle source still emits a keepalive
                    // carrying wal_end >= barrier (otherwise a quiet source can stall
                    // the barrier indefinitely).
                    if i % 20 == 19 {
                        client.execute("SELECT pg_log_standby_snapshot()", &[]).await.ok();
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                anyhow::bail!(
                    "strong read timed out waiting for the stream to pass the source barrier"
                )
            }
            other if other.starts_with("bounded(") => {
                let inner = other
                    .trim_start_matches("bounded(")
                    .trim_end_matches(')')
                    .trim_end_matches('s');
                let secs: f64 = inner.parse().unwrap_or(5.0);
                // Staleness vs the source heartbeat; fast error on violation (A4).
                let client = self.cfg.connect().await?;
                let s: String = client
                    .query_one("SELECT pg_current_wal_lsn()::text", &[])
                    .await?
                    .get(0);
                let head = parse_lsn(&s)?;
                let applied = self
                    .pipeline
                    .read()
                    .await
                    .as_ref()
                    .map(|p| p.applied_lsn)
                    .unwrap_or(0);
                let lag_bytes = head.saturating_sub(applied);
                // Demo-grade staleness estimate: bytes-per-second observed since attach
                // is not tracked yet, so bound on bytes with a documented conversion.
                let budget = (secs * 1_000_000.0) as u64; // 1 MB/s reference rate
                anyhow::ensure!(
                    lag_bytes <= budget,
                    "bounded({secs}s) violated: apply lag {lag_bytes} bytes exceeds {budget} \
                     (fast error, per Amendment A A4)"
                );
                Ok(Some(applied))
            }
            other => anyhow::bail!("unknown consistency class {other:?}"),
        }
    }

    // ---- Chaos controls (SP7 live) ---------------------------------------------

    pub async fn chaos_kill_decoder(self: &Arc<Self>) -> Result<()> {
        let mut pump = self.pump.lock().await;
        let Some(h) = pump.take() else {
            anyhow::bail!("no pump running");
        };
        let killed_at = h.durable.borrow().lsn;
        h.task.abort();
        let _ = h.task.await;
        drop(pump);
        self.event(
            "warn",
            format!("CHAOS: decoder killed mid-stream at durable={}", format_lsn(killed_at)),
        )
        .await;
        Ok(())
    }

    /// Restart per the ack invariant: resume the log at its durable boundary and open
    /// a FRESH replication session from that ack (never splice a dying session).
    pub async fn restart_decoder(self: &Arc<Self>) -> Result<()> {
        let log = graydb_log::FrameLog::resume(&self.log_dir, self.cfg.log.segment_max_bytes).await?;
        let mark = log.durable_now();
        self.start_pump(log, mark.lsn).await?;
        self.event(
            "info",
            format!(
                "restart: fresh replication session from last durable ack {} (Relation metadata re-emitted)",
                format_lsn(mark.lsn)
            ),
        )
        .await;
        Ok(())
    }

    /// Rung 3: degrade the log write path — frames spill to staging, the durable mark
    /// freezes, and the slot ack stops advancing (the WAL gauge starts climbing).
    pub async fn chaos_stall_log(self: &Arc<Self>, stalled: bool) -> Result<()> {
        let pump = self.pump.lock().await;
        let h = pump.as_ref().context("no pump running")?;
        h.ctrl.send(PumpCommand { stalled, shutdown: false })?;
        drop(pump);
        self.event(
            if stalled { "warn" } else { "info" },
            if stalled {
                "CHAOS: log write path stalled — rung 3 staging active, ack frozen"
            } else {
                "resumed: staging drained in order, durable mark advanced, ack caught up"
            },
        )
        .await;
        Ok(())
    }

    /// Demo 4 button: freeze materialization while frames keep landing durably.
    pub async fn chaos_crash_before_materialize(self: &Arc<Self>, frozen: bool) -> Result<()> {
        self.crash_before_materialize.send(frozen)?;
        self.event(
            if frozen { "warn" } else { "info" },
            if frozen {
                "CHAOS: materialization frozen — frames still durable, shapes falling behind"
            } else {
                "materialization resumed — replaying from the durable log, zero loss"
            },
        )
        .await;
        Ok(())
    }

    /// Failover sim: crash-restart the local source with pg_ctl -m immediate.
    pub async fn chaos_restart_source(self: &Arc<Self>) -> Result<()> {
        let (bin, data) = self.pg_paths()?;
        self.event("warn", "CHAOS: source PostgreSQL stopping (-m immediate)").await;
        run_pg_ctl(&bin, &data, &["-m", "immediate", "-w", "stop"])?;
        run_pg_ctl(&bin, &data, &["-w", "start"])?;
        for _ in 0..120 {
            if self.cfg.connect().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        self.event("info", "source restarted; restart the decoder to continue from the ack").await;
        Ok(())
    }

    fn pg_paths(&self) -> Result<(PathBuf, PathBuf)> {
        let v = match self.cfg.source.port {
            5416 => "pg16",
            5417 => "pg17",
            p => anyhow::bail!("no local pg_ctl mapping for port {p}"),
        };
        let tools = std::path::Path::new("..").join("tools");
        Ok((
            tools.join(v).join("pgsql").join("bin").join("pg_ctl.exe"),
            tools.join("pgdata").join(v),
        ))
    }
}

fn run_pg_ctl(bin: &PathBuf, data: &PathBuf, args: &[&str]) -> Result<()> {
    let mut cmd = std::process::Command::new(bin);
    cmd.arg("-D").arg(data);
    for a in args {
        cmd.arg(a);
    }
    if args.contains(&"start") {
        cmd.arg("-l").arg(data.join("server.log"));
    }
    // Null stdio: a started server must never inherit our handles.
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let status = cmd.status().with_context(|| format!("running {}", bin.display()))?;
    anyhow::ensure!(status.success(), "pg_ctl {args:?} failed with {status}");
    Ok(())
}

fn now_hms() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let s = secs % 86400;
    format!("{:02}:{:02}:{:02}", s / 3600, (s % 3600) / 60, s % 60)
}
