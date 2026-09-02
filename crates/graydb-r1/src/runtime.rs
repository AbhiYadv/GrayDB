//! Operational Task 12 runtime.
//!
//! `RunController` owns durability and ordering.  This module owns the live
//! stage mechanics and keeps every process, PostgreSQL, HTTP, clock, and disk
//! interaction behind injectable ports.  The same scheduler is therefore used
//! by the operator runtime and deterministic service fakes.

use crate::adapter::{EngineAdapter, QueryInvocation, QueryResult};
use crate::artifacts::sha256_tree;
use crate::contracts::{EngineKind, LogicalCheckpoint, RunMode};
use crate::controller::{
    pause_for_free_space, rate_search_stop, search_rates, BenchmarkRuntime, CommandOutcome,
    IsolatedReplayEvidence, QueryStagePolicy, RateSearchObservation, RunPlan, RunStage,
    StageContext, StageOutcome, StageQueryRecord,
};
use crate::query::{canonical_digest, QueryId, QuerySchedule};
use crate::verdict::RunInvalidation;
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};

const QUERY_ATTEMPTS_PER_SCHEDULED_WINDOW: u32 = 300;
const RATE_SAMPLE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuntimeStageEvidence {
    pub command_outcomes: Vec<CommandOutcome>,
    pub artifact_paths: Vec<String>,
}

impl RuntimeStageEvidence {
    fn into_outcome(self) -> StageOutcome {
        StageOutcome {
            command_outcomes: self.command_outcomes,
            artifact_paths: self.artifact_paths,
            ..StageOutcome::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskSpaceSample {
    pub total_bytes: u64,
    pub free_bytes: u64,
}

/// Time is a port because the scheduler must be tested with its exact duration,
/// cadence, and free-space sampling rules without shortening production values.
#[async_trait]
pub trait RuntimeClock: Send + Sync {
    fn elapsed(&self) -> Duration;
    fn unix_ms(&self) -> u128;
    async fn sleep(&self, duration: Duration);
}

pub struct SystemRuntimeClock {
    started: Instant,
}

impl Default for SystemRuntimeClock {
    fn default() -> Self {
        Self {
            started: Instant::now(),
        }
    }
}

#[async_trait]
impl RuntimeClock for SystemRuntimeClock {
    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    fn unix_ms(&self) -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// One shell-free host operation. Environment values are intentionally absent
/// from [`CommandOutcome`] so database passwords never enter the run bundle.
#[derive(Debug, Clone)]
pub struct RuntimeProcessRequest {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub environment: BTreeMap<String, String>,
}

pub trait RuntimeProcess: Send + Sync {
    fn run(&self, request: &RuntimeProcessRequest) -> Result<CommandOutcome>;
}

#[derive(Debug, Default)]
pub struct SystemRuntimeProcess;

impl RuntimeProcess for SystemRuntimeProcess {
    fn run(&self, request: &RuntimeProcessRequest) -> Result<CommandOutcome> {
        let mut command = std::process::Command::new(&request.program);
        command.args(&request.args).envs(&request.environment);
        if let Some(cwd) = &request.cwd {
            command.current_dir(cwd);
        }
        let output = command
            .output()
            .with_context(|| format!("starting {}", request.program))?;
        Ok(CommandOutcome {
            program: request.program.clone(),
            args: request.args.clone(),
            exit_code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

#[async_trait]
pub trait RuntimePostgresConnector: Send + Sync {
    async fn connect(&self, database_url: &str) -> Result<Client>;
}

#[derive(Debug, Default)]
pub struct TokioPostgresConnector;

#[async_trait]
impl RuntimePostgresConnector for TokioPostgresConnector {
    async fn connect(&self, database_url: &str) -> Result<Client> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls)
            .await
            .with_context(|| format!("connecting to PostgreSQL at {}", redact_url(database_url)))?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "R1 PostgreSQL connection terminated");
            }
        });
        Ok(client)
    }
}

#[async_trait]
pub trait RuntimeEngines: Send + Sync {
    async fn query(&self, engine: EngineKind, invocation: &QueryInvocation) -> Result<QueryResult>;
    async fn status(&self, engine: EngineKind) -> Result<crate::adapter::EngineStatus>;
    async fn wait_visible(
        &self,
        engine: EngineKind,
        target_lsn: u64,
        timeout: Duration,
    ) -> Result<Duration>;
    async fn bootstrap_clickhouse(&self) -> Result<()>;
    async fn load_clickhouse_snapshot(
        &self,
        postgres_host: &str,
        postgres_port: u16,
        postgres_database: &str,
        postgres_user: &str,
        postgres_password: &str,
        source_lsn: u64,
    ) -> Result<()>;
    async fn replay_count(&self, engine: EngineKind) -> Result<u64>;
    async fn operation_anomalies(&self, engine: EngineKind) -> Result<(u64, u64, u64)>;
    async fn start_clickhouse_cdc(
        &self,
        config: &SystemRuntimeConfig,
        run_root: &Path,
    ) -> Result<ClickHouseCdcTask>;
}

#[derive(Debug, Clone)]
pub struct SystemRuntimeConfig {
    pub repository_root: PathBuf,
    pub compose_file: PathBuf,
    pub project_name: String,
    pub postgres_url: String,
    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_database: String,
    pub postgres_user: String,
    pub postgres_password: String,
    pub seed: u64,
}

impl SystemRuntimeConfig {
    pub fn from_env() -> Result<Self> {
        let repository_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let postgres_url = std::env::var("GRAYDB_R1_POSTGRES_URL")
            .unwrap_or_else(|_| "postgres://r1:graydb_r1@127.0.0.1:55432/r1".into());
        let (postgres_host, postgres_port, postgres_user, postgres_password, postgres_database) =
            parse_postgres_url(&postgres_url)?;
        Ok(Self {
            compose_file: repository_root.join("bench/r1/compose.yml"),
            repository_root,
            project_name: "graydb-r1".into(),
            postgres_url,
            postgres_host,
            postgres_port,
            postgres_database,
            postgres_user,
            postgres_password,
            seed: 20_260_901,
        })
    }
}

struct ActiveWriter {
    target_rows_per_sec: u64,
    stop: watch::Sender<bool>,
    join: JoinHandle<(crate::replication::ApplicationWriter, Result<()>)>,
}

#[derive(Default)]
struct WriterCoordinator {
    writer: Option<crate::replication::ApplicationWriter>,
    active: Option<ActiveWriter>,
    replication_stop: Option<watch::Sender<bool>>,
    replication_join: Option<JoinHandle<Result<()>>>,
}

impl WriterCoordinator {
    fn active_rate(&self) -> Option<u64> {
        self.active
            .as_ref()
            .map(|active| active.target_rows_per_sec)
    }

    async fn ensure_initialized(
        &mut self,
        run_root: &Path,
        config: &SystemRuntimeConfig,
        connector: &dyn RuntimePostgresConnector,
    ) -> Result<()> {
        if self.writer.is_some() || self.active.is_some() {
            return Ok(());
        }
        let (mapped_tx, mapped_rx) = tokio::sync::mpsc::channel(1_024);
        let (replication_stop, replication_stop_rx) = watch::channel(false);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let replication_config = crate::replication::ControlReplicationConfig {
            host: config.postgres_host.clone(),
            port: config.postgres_port,
            user: config.postgres_user.clone(),
            password: config.postgres_password.clone(),
            database: config.postgres_database.clone(),
            initial_lsn: 0,
            frame_log_dir: run_root.join("control-frame-log"),
            segment_max_bytes: 64 << 20,
        };
        let replication_join = tokio::spawn(async move {
            crate::replication::run_control_replication_with_ready(
                replication_config,
                mapped_tx,
                replication_stop_rx,
                Some(ready_tx),
            )
            .await
        });
        match tokio::time::timeout(Duration::from_secs(30), ready_rx).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => bail!("control replication failed to start: {error}"),
            Ok(Err(_)) => bail!("control replication exited before reporting readiness"),
            Err(_) => bail!("control replication did not become ready within 30 seconds"),
        }
        let client = connector.connect(&config.postgres_url).await?;
        let recovery = Arc::new(crate::replication::PostgresCommitRecovery::new(
            config.postgres_url.clone(),
        ));
        let intents = crate::ledger::IntentLog::create(run_root)?;
        let ledger = crate::ledger::CommittedLedger::resume(run_root)?;
        self.writer = Some(crate::replication::ApplicationWriter::new(
            client,
            recovery,
            crate::workload::WorkloadPlanner::new(config.seed),
            intents,
            ledger,
            mapped_rx,
        ));
        self.replication_stop = Some(replication_stop);
        self.replication_join = Some(replication_join);
        Ok(())
    }

    async fn start(
        &mut self,
        target_rows_per_sec: u64,
        run_root: &Path,
        config: &SystemRuntimeConfig,
        connector: &dyn RuntimePostgresConnector,
    ) -> Result<()> {
        if self.active_rate() == Some(target_rows_per_sec) {
            return Ok(());
        }
        if self.active.is_some() {
            self.stop().await?;
        }
        self.ensure_initialized(run_root, config, connector).await?;
        let mut writer = self.writer.take().context("writer was not initialized")?;
        let (stop, stop_rx) = watch::channel(false);
        let join = tokio::spawn(async move {
            let result = writer.run(target_rows_per_sec, stop_rx).await;
            (writer, result)
        });
        self.active = Some(ActiveWriter {
            target_rows_per_sec,
            stop,
            join,
        });
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        active
            .stop
            .send(true)
            .map_err(|_| anyhow::anyhow!("writer stop channel closed"))?;
        let (writer, result) = active.join.await.context("joining application writer")?;
        self.writer = Some(writer);
        result.context("application writer stopped with an error")
    }
}

impl Drop for WriterCoordinator {
    fn drop(&mut self) {
        if let Some(active) = &self.active {
            let _ = active.stop.send(true);
        }
        if let Some(stop) = &self.replication_stop {
            let _ = stop.send(true);
        }
        if let Some(join) = &self.replication_join {
            join.abort();
        }
    }
}

#[derive(Debug, Clone)]
struct RateWindow {
    target_rows_per_sec: u64,
    started: Instant,
    starting_rows: u64,
    previous_backlog: u64,
}

pub struct ClickHouseCdcTask {
    stop: watch::Sender<bool>,
    join: JoinHandle<Result<()>>,
}

impl ClickHouseCdcTask {
    async fn stop(self) -> Result<()> {
        let _ = self.stop.send(true);
        self.join.await.context("joining ClickHouse CDC task")?
    }
}

/// The real Task 12 service binding.  It composes the Task 4 loader, Task 5/6
/// intent-ledger writer, Task 7/8 HTTP adapters, Task 9 checkpoint protocol,
/// Task 10 report types, and the shell-free Task 11 Compose topology.
pub struct SystemR1RuntimeServices<P, C, E>
where
    P: RuntimeProcess,
    C: RuntimePostgresConnector,
    E: RuntimeEngines,
{
    config: SystemRuntimeConfig,
    process: Arc<P>,
    connector: Arc<C>,
    engines: Arc<E>,
    writer: Arc<tokio::sync::Mutex<WriterCoordinator>>,
    rate_window: Option<RateWindow>,
    active_isolated_engine: Option<EngineKind>,
    run_root: Option<PathBuf>,
    active_plan: Option<RunPlan>,
    clickhouse_cdc: Option<ClickHouseCdcTask>,
}

impl SystemR1RuntimeServices<SystemRuntimeProcess, TokioPostgresConnector, HttpEngines> {
    pub fn from_env() -> Result<Self> {
        let config = SystemRuntimeConfig::from_env()?;
        let graydb_url = std::env::var("GRAYDB_R1_GRAYDB_HTTP")
            .unwrap_or_else(|_| "http://127.0.0.1:57432".into());
        let clickhouse_url =
            std::env::var("CLICKHOUSE_HTTP").unwrap_or_else(|_| "http://127.0.0.1:58123".into());
        Ok(Self::new(
            config,
            Arc::new(SystemRuntimeProcess),
            Arc::new(TokioPostgresConnector),
            Arc::new(HttpEngines::new(graydb_url, clickhouse_url)),
        ))
    }
}

impl<P, C, E> SystemR1RuntimeServices<P, C, E>
where
    P: RuntimeProcess,
    C: RuntimePostgresConnector,
    E: RuntimeEngines,
{
    pub fn new(
        config: SystemRuntimeConfig,
        process: Arc<P>,
        connector: Arc<C>,
        engines: Arc<E>,
    ) -> Self {
        Self {
            config,
            process,
            connector,
            engines,
            writer: Arc::new(tokio::sync::Mutex::new(WriterCoordinator::default())),
            rate_window: None,
            active_isolated_engine: None,
            run_root: None,
            active_plan: None,
            clickhouse_cdc: None,
        }
    }

    fn service_root(run_root: &Path) -> PathBuf {
        run_root.join("services")
    }

    fn compose_request(
        &self,
        run_root: &Path,
        data_root: &Path,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> RuntimeProcessRequest {
        let mut command_args = vec![
            "compose".into(),
            "--project-name".into(),
            self.config.project_name.clone(),
            "--file".into(),
            self.config.compose_file.display().to_string(),
        ];
        command_args.extend(args.into_iter().map(Into::into));
        RuntimeProcessRequest {
            program: "docker".into(),
            args: command_args,
            cwd: Some(self.config.repository_root.clone()),
            environment: BTreeMap::from([
                ("R1_DATA_ROOT".into(), data_root.display().to_string()),
                (
                    "R1_GIT_SHA".into(),
                    frozen_git_sha(run_root).unwrap_or_else(|| "unrecorded".into()),
                ),
            ]),
        }
    }

    fn run_checked(&self, request: RuntimeProcessRequest) -> Result<CommandOutcome> {
        let outcome = self.process.run(&request)?;
        if !outcome.is_success() {
            bail!(
                "{} {:?} failed with {:?}: {}",
                outcome.program,
                outcome.args,
                outcome.exit_code,
                outcome.stderr.trim()
            );
        }
        Ok(outcome)
    }

    async fn wait_postgres(&self) -> Result<Client> {
        let started = Instant::now();
        loop {
            match self.connector.connect(&self.config.postgres_url).await {
                Ok(client) => return Ok(client),
                Err(error) if started.elapsed() < Duration::from_secs(60) => {
                    tracing::debug!(%error, "waiting for R1 PostgreSQL");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(error) => {
                    return Err(error.context("PostgreSQL was not ready within 60 seconds"))
                }
            }
        }
    }

    async fn current_source_lsn(&self) -> Result<u64> {
        let client = self.connector.connect(&self.config.postgres_url).await?;
        postgres_lsn(&client).await
    }

    async fn committed_rows(&self, run_root: &Path) -> Result<u64> {
        committed_rows(run_root)
    }
}

impl<P, C, E> Drop for SystemR1RuntimeServices<P, C, E>
where
    P: RuntimeProcess,
    C: RuntimePostgresConnector,
    E: RuntimeEngines,
{
    fn drop(&mut self) {
        if let Some(cdc) = &self.clickhouse_cdc {
            let _ = cdc.stop.send(true);
            cdc.join.abort();
        }
    }
}

/// Database/HTTP/process port consumed by [`MacComposeRuntime`].  Methods are
/// intentionally operational rather than returning synthetic metrics: the
/// scheduler chooses checkpoints and invokes every Q1-Q5 request itself.
#[async_trait]
pub trait R1RuntimeServices: Send {
    fn bind_run(&mut self, _run_root: &Path, _plan: &RunPlan) -> Result<()> {
        Ok(())
    }
    /// Switches the concrete source restore and analytical service used for an
    /// isolated measurement. Correctness runtimes keep the default no-op.
    async fn activate_isolated_engine(
        &mut self,
        _run_root: &Path,
        _plan: &RunPlan,
        _engine: EngineKind,
    ) -> Result<()> {
        Ok(())
    }
    async fn preflight(&mut self, run_root: &Path, plan: &RunPlan) -> Result<RuntimeStageEvidence>;
    async fn seed(&mut self, run_root: &Path, plan: &RunPlan) -> Result<RuntimeStageEvidence>;
    async fn capture_baseline(
        &mut self,
        run_root: &Path,
        plan: &RunPlan,
    ) -> Result<RuntimeStageEvidence>;
    async fn prepare_isolated_replay(
        &mut self,
        run_root: &Path,
        plan: &RunPlan,
    ) -> Result<IsolatedReplayEvidence>;
    async fn bootstrap(&mut self, run_root: &Path, plan: &RunPlan) -> Result<RuntimeStageEvidence>;
    async fn checkpoint(
        &mut self,
        run_root: &Path,
        stage: RunStage,
        plan: &RunPlan,
    ) -> Result<RuntimeStageEvidence>;
    async fn set_writer_rate(&mut self, target_rows_per_sec: Option<u64>) -> Result<()>;
    async fn query_checkpoint(
        &mut self,
        mode: RunMode,
        engine: EngineKind,
    ) -> Result<LogicalCheckpoint>;
    async fn query(
        &mut self,
        engine: EngineKind,
        invocation: QueryInvocation,
    ) -> Result<QueryResult>;
    async fn rate_observation(&mut self, target_rows_per_sec: u64)
        -> Result<RateSearchObservation>;
    async fn disk_space(&mut self, run_root: &Path) -> Result<DiskSpaceSample>;
    async fn failure_sequence(
        &mut self,
        run_root: &Path,
        plan: &RunPlan,
    ) -> Result<RuntimeStageEvidence>;
    async fn report(
        &mut self,
        run_root: &Path,
        plan: &RunPlan,
        invalidations: &[RunInvalidation],
    ) -> Result<RuntimeStageEvidence>;
    async fn checksums(&mut self, run_root: &Path) -> Result<RuntimeStageEvidence>;
}

#[async_trait]
impl<P, C, E> R1RuntimeServices for SystemR1RuntimeServices<P, C, E>
where
    P: RuntimeProcess + 'static,
    C: RuntimePostgresConnector + 'static,
    E: RuntimeEngines + 'static,
{
    fn bind_run(&mut self, run_root: &Path, plan: &RunPlan) -> Result<()> {
        if let Some(existing) = &self.run_root {
            anyhow::ensure!(existing == run_root, "runtime cannot switch run roots");
        }
        self.run_root = Some(run_root.to_path_buf());
        self.active_plan = Some(plan.clone());
        Ok(())
    }

    async fn preflight(&mut self, run_root: &Path, plan: &RunPlan) -> Result<RuntimeStageEvidence> {
        fs::create_dir_all(Self::service_root(run_root))?;
        let snapshot = crate::preflight::PreflightProbe::probe(
            &crate::preflight::SystemPreflightProbe::default(),
            run_root,
        )?;
        let mut snapshot = snapshot;
        snapshot.expected_peak_bytes = plan.spec.minimum_bytes.saturating_mul(4);
        snapshot.runtime_stop_bytes = plan.spec.minimum_bytes;
        let report = crate::preflight::PreflightPolicy::r1_mac().evaluate(&snapshot);
        write_json_atomic(&run_root.join("preflight-report.json"), &report)?;
        if !report.passed {
            let failures = report
                .failures
                .iter()
                .map(|failure| format!("{}: {}", failure.code, failure.message))
                .collect::<Vec<_>>()
                .join("; ");
            bail!("R1 preflight failed: {failures}");
        }
        let outcome = self.run_checked(self.compose_request(
            run_root,
            &Self::service_root(run_root),
            ["config", "--quiet"],
        ))?;
        Ok(RuntimeStageEvidence {
            command_outcomes: vec![outcome],
            artifact_paths: vec!["environment.json".into(), "preflight-report.json".into()],
        })
    }

    async fn seed(&mut self, run_root: &Path, plan: &RunPlan) -> Result<RuntimeStageEvidence> {
        let manifest_path = run_root.join("dataset-manifest.json");
        if manifest_path.metadata().map(|meta| meta.len()).unwrap_or(0) > 0 {
            let _: crate::manifest::DatasetManifest =
                serde_json::from_slice(&fs::read(&manifest_path)?)
                    .context("validating existing immutable dataset manifest")?;
            return Ok(RuntimeStageEvidence {
                artifact_paths: vec!["dataset-manifest.json".into()],
                ..RuntimeStageEvidence::default()
            });
        }
        let service_root = Self::service_root(run_root);
        fs::create_dir_all(&service_root)?;
        let compose = self.run_checked(self.compose_request(
            run_root,
            &service_root,
            ["up", "--detach", "--wait", "postgres"],
        ))?;
        let admin = self.wait_postgres().await?;
        let schema_exists: bool = admin
            .query_one("SELECT to_regnamespace('r1') IS NOT NULL", &[])
            .await?
            .get(0);
        if schema_exists {
            admin
                .batch_execute(
                    "TRUNCATE r1.order_events, r1.orders, r1.customers, r1.tenants, r1_control.tx_marker",
                )
                .await
                .context("resetting an incomplete run-scoped seed")?;
        } else {
            admin
                .batch_execute(include_str!("../../../bench/r1/schema.sql"))
                .await
                .context("installing R1 PostgreSQL schema")?;
        }
        let loader = crate::manifest::DatasetLoader::with_probe(
            crate::manifest::PostgresPublishedSizeProbe::new(
                self.connector.connect(&self.config.postgres_url).await?,
            ),
            crate::manifest::PostgresCopySink::new(
                self.connector.connect(&self.config.postgres_url).await?,
            ),
            self.config.seed,
        );
        let manifest = loader.load_until(plan.spec.minimum_bytes).await?;
        manifest.write_immutable(run_root)?;
        write_json_atomic(
            &run_root.join("workload-manifest.json"),
            &serde_json::json!({
                "seed": self.config.seed,
                "intent_log": "workload-intents.jsonl",
                "committed_ledger": "workload-ledger.jsonl",
                "dataset_content_sha256": manifest.content_hash()?,
            }),
        )?;
        Ok(RuntimeStageEvidence {
            command_outcomes: vec![compose],
            artifact_paths: vec![
                "dataset-manifest.json".into(),
                "workload-manifest.json".into(),
            ],
        })
    }

    async fn capture_baseline(
        &mut self,
        run_root: &Path,
        _plan: &RunPlan,
    ) -> Result<RuntimeStageEvidence> {
        let destination = baseline_path(run_root);
        if destination.exists() {
            let checksum = destination.join("SHA256SUMS");
            anyhow::ensure!(
                checksum.is_file(),
                "baseline exists without its immutable checksum at {}",
                checksum.display()
            );
            verify_checksum_file(&destination, &checksum)?;
            return Ok(RuntimeStageEvidence {
                artifact_paths: vec!["baseline/postgres/SHA256SUMS".into()],
                ..RuntimeStageEvidence::default()
            });
        }
        fs::create_dir_all(destination.parent().context("baseline has no parent")?)?;
        let request = RuntimeProcessRequest {
            program: "pg_basebackup".into(),
            args: vec![
                "--host".into(),
                self.config.postgres_host.clone(),
                "--port".into(),
                self.config.postgres_port.to_string(),
                "--username".into(),
                self.config.postgres_user.clone(),
                "--dbname".into(),
                self.config.postgres_database.clone(),
                "--pgdata".into(),
                destination.display().to_string(),
                "--checkpoint=fast".into(),
                "--wal-method=stream".into(),
            ],
            cwd: Some(self.config.repository_root.clone()),
            environment: BTreeMap::from([(
                "PGPASSWORD".into(),
                self.config.postgres_password.clone(),
            )]),
        };
        let outcome = self.run_checked(request)?;
        anyhow::ensure!(
            destination.is_dir(),
            "pg_basebackup created no baseline directory"
        );
        let checksum = sha256_tree(&destination)?;
        Ok(RuntimeStageEvidence {
            command_outcomes: vec![outcome],
            artifact_paths: vec![path_relative(run_root, &checksum)],
        })
    }

    async fn prepare_isolated_replay(
        &mut self,
        run_root: &Path,
        plan: &RunPlan,
    ) -> Result<IsolatedReplayEvidence> {
        prepare_system_isolated_replay(
            &self.config,
            self.process.as_ref(),
            self.connector.as_ref(),
            run_root,
            plan,
        )
        .await
    }

    async fn bootstrap(&mut self, run_root: &Path, plan: &RunPlan) -> Result<RuntimeStageEvidence> {
        if plan.mode == RunMode::Isolated {
            write_json_atomic(
                &run_root.join("configs/runtime.json"),
                &runtime_config_record(&self.config, plan, None),
            )?;
            return Ok(RuntimeStageEvidence {
                artifact_paths: vec!["configs/runtime.json".into()],
                ..RuntimeStageEvidence::default()
            });
        }
        let service_root = Self::service_root(run_root);
        let compose = self.run_checked(self.compose_request(
            run_root,
            &service_root,
            [
                "up",
                "--detach",
                "--wait",
                "postgres",
                "graydb",
                "clickhouse",
            ],
        ))?;
        self.engines.bootstrap_clickhouse().await?;
        let source_lsn = manifest_initial_lsn(run_root)?;
        self.engines
            .load_clickhouse_snapshot(
                "postgres",
                5432,
                &self.config.postgres_database,
                &self.config.postgres_user,
                &self.config.postgres_password,
                source_lsn,
            )
            .await?;
        for engine in &plan.engines {
            self.engines
                .wait_visible(*engine, source_lsn, Duration::from_secs(30 * 60))
                .await
                .with_context(|| format!("waiting for {engine:?} initial snapshot"))?;
        }
        if plan.engines.contains(&EngineKind::Clickhouse) {
            self.clickhouse_cdc = Some(
                self.engines
                    .start_clickhouse_cdc(&self.config, run_root)
                    .await?,
            );
        }
        write_json_atomic(
            &run_root.join("configs/runtime.json"),
            &runtime_config_record(&self.config, plan, None),
        )?;
        Ok(RuntimeStageEvidence {
            command_outcomes: vec![compose],
            artifact_paths: vec!["configs/runtime.json".into()],
        })
    }

    async fn checkpoint(
        &mut self,
        run_root: &Path,
        stage: RunStage,
        plan: &RunPlan,
    ) -> Result<RuntimeStageEvidence> {
        let path = run_root.join(format!("correctness/{stage:?}.json"));
        let record = if plan.mode == RunMode::Correctness {
            capture_runtime_checkpoint(
                self.connector.as_ref(),
                self.engines.as_ref(),
                &self.writer,
                run_root,
                &self.config,
                plan,
            )
            .await?
        } else {
            let mut checkpoints = Vec::new();
            let mut differences = Vec::new();
            let mut logical_sequence = None;
            for engine in plan.engines.clone() {
                activate_system_isolated_engine(self, run_root, plan, engine).await?;
                let sequence = self
                    .query_checkpoint(RunMode::Isolated, engine)
                    .await?
                    .sequence;
                let mut engine_plan = plan.clone();
                engine_plan.engines = vec![engine];
                let bundle = capture_runtime_checkpoint(
                    self.connector.as_ref(),
                    self.engines.as_ref(),
                    &self.writer,
                    run_root,
                    &self.config,
                    &engine_plan,
                )
                .await?;
                if let Some(expected) = logical_sequence {
                    if expected != sequence {
                        differences.push(crate::oracle::RowDifference {
                            table: "isolated-replay".into(),
                            primary_key: 0,
                            expected_version: expected,
                            actual_version: sequence,
                            target_checkpoint: expected,
                            detail: "isolated engines reached different logical checkpoints".into(),
                        });
                    }
                } else {
                    logical_sequence = Some(sequence);
                }
                differences.extend(bundle.verdict.differences);
                checkpoints.extend(bundle.checkpoints);
            }
            let verdict = crate::oracle::CorrectnessVerdict {
                passed: differences.is_empty(),
                invalidations: differences
                    .iter()
                    .map(|_| RunInvalidation::WorkloadHashMismatch)
                    .collect(),
                differences,
            };
            RuntimeCheckpointBundle {
                mode: plan.mode,
                checkpoints,
                verdict,
            }
        };
        write_json_atomic(&path, &record)?;
        anyhow::ensure!(
            record.verdict.passed,
            "{stage:?} correctness checkpoint failed"
        );
        Ok(RuntimeStageEvidence {
            artifact_paths: vec![path_relative(run_root, &path)],
            ..RuntimeStageEvidence::default()
        })
    }

    async fn set_writer_rate(&mut self, target_rows_per_sec: Option<u64>) -> Result<()> {
        let run_root = self
            .run_root
            .as_deref()
            .context("runtime stage did not bind its run root")?;
        match target_rows_per_sec {
            Some(target) => {
                let starting_rows = self.committed_rows(run_root).await?;
                self.writer
                    .lock()
                    .await
                    .start(target, run_root, &self.config, self.connector.as_ref())
                    .await?;
                self.rate_window = Some(RateWindow {
                    target_rows_per_sec: target,
                    started: Instant::now(),
                    starting_rows,
                    previous_backlog: 0,
                });
                Ok(())
            }
            None => {
                let result = self.writer.lock().await.stop().await;
                self.rate_window = None;
                result
            }
        }
    }

    async fn query_checkpoint(
        &mut self,
        mode: RunMode,
        engine: EngineKind,
    ) -> Result<LogicalCheckpoint> {
        let run_root = self
            .run_root
            .as_deref()
            .context("runtime stage did not bind its run root")?;
        if mode == RunMode::Isolated {
            let map = crate::replication::ReplayMap::resume(
                run_root.join("isolated").join(engine_name(engine)),
            )?;
            let entry = map
                .entries()
                .last()
                .context("isolated replay map has no committed checkpoint")?;
            return Ok(LogicalCheckpoint {
                sequence: entry.logical_sequence,
                source_lsn: entry.replay_source_lsn,
            });
        }
        let ledger = crate::ledger::CommittedLedger::resume(run_root)?;
        // Correctness checkpoints come from the committed ledger, not a fresh
        // WAL read: the ledger's last entry pairs the sequence with its
        // commit-end source LSN, is identical for every engine, and never
        // races the active writer.
        let last = ledger.entries().last().cloned();
        Ok(LogicalCheckpoint {
            sequence: last.as_ref().map(|entry| entry.sequence).unwrap_or(0),
            source_lsn: last.as_ref().map(|entry| entry.source_lsn).unwrap_or(0),
        })
    }

    async fn query(
        &mut self,
        engine: EngineKind,
        invocation: QueryInvocation,
    ) -> Result<QueryResult> {
        self.engines.query(engine, &invocation).await
    }

    async fn rate_observation(
        &mut self,
        target_rows_per_sec: u64,
    ) -> Result<RateSearchObservation> {
        let run_root = self
            .run_root
            .as_deref()
            .context("runtime stage did not bind its run root")?;
        let plan = self
            .active_plan
            .as_ref()
            .context("runtime stage did not bind its run plan")?;
        let current_rows = self.committed_rows(run_root).await?;
        let source_lsn = self.current_source_lsn().await?;
        let window = self
            .rate_window
            .as_mut()
            .context("rate observation requested while writer is stopped")?;
        anyhow::ensure!(
            window.target_rows_per_sec == target_rows_per_sec,
            "rate observation target differs from active writer rate"
        );
        let elapsed = window.started.elapsed().as_secs_f64().max(0.001);
        let achieved_rows_per_sec =
            ((current_rows.saturating_sub(window.starting_rows) as f64) / elapsed).round() as u64;
        let mut freshness_p99_ms = 0;
        let mut minimum_applied = source_lsn;
        let mut correctness_passed = true;
        for engine in &plan.engines {
            let status = self.engines.status(*engine).await?;
            correctness_passed &= status.healthy;
            freshness_p99_ms = freshness_p99_ms.max(status.lag_ms.unwrap_or(0));
            minimum_applied = minimum_applied.min(
                status
                    .applied_lsn
                    .with_context(|| format!("{engine:?} status omitted applied LSN"))?,
            );
        }
        let backlog_bytes = source_lsn.saturating_sub(minimum_applied);
        let backlog_growing = backlog_bytes > window.previous_backlog;
        window.previous_backlog = backlog_bytes;
        Ok(RateSearchObservation {
            target_rows_per_sec,
            achieved_rows_per_sec,
            freshness_p99_ms,
            backlog_bytes,
            backlog_growing,
            correctness_passed,
            resource_gate: None,
        })
    }

    async fn disk_space(&mut self, run_root: &Path) -> Result<DiskSpaceSample> {
        Ok(DiskSpaceSample {
            total_bytes: fs2::total_space(run_root)?,
            free_bytes: fs2::available_space(run_root)?,
        })
    }

    async fn failure_sequence(
        &mut self,
        run_root: &Path,
        plan: &RunPlan,
    ) -> Result<RuntimeStageEvidence> {
        run_system_failure_sequence(self, run_root, plan).await
    }

    async fn report(
        &mut self,
        run_root: &Path,
        plan: &RunPlan,
        invalidations: &[RunInvalidation],
    ) -> Result<RuntimeStageEvidence> {
        write_system_report(run_root, plan, invalidations)
    }

    async fn checksums(&mut self, run_root: &Path) -> Result<RuntimeStageEvidence> {
        write_runtime_checksums(run_root)
    }

    async fn activate_isolated_engine(
        &mut self,
        run_root: &Path,
        plan: &RunPlan,
        engine: EngineKind,
    ) -> Result<()> {
        activate_system_isolated_engine(self, run_root, plan, engine).await
    }
}

/// Concrete stage runner shared by the real Mac binding and fake-runtime tests.
pub struct MacComposeRuntime<S, C> {
    services: S,
    clock: C,
}

impl<S, C> MacComposeRuntime<S, C> {
    pub fn new(services: S, clock: C) -> Self {
        Self { services, clock }
    }

    pub fn services(&self) -> &S {
        &self.services
    }

    pub fn services_mut(&mut self) -> &mut S {
        &mut self.services
    }

    pub fn into_services(self) -> S {
        self.services
    }
}

#[async_trait]
impl<S, C> BenchmarkRuntime for MacComposeRuntime<S, C>
where
    S: R1RuntimeServices,
    C: RuntimeClock,
{
    async fn execute_stage(&mut self, context: StageContext<'_>) -> Result<StageOutcome> {
        let root = context.run_root;
        let plan = context.plan;
        self.services.bind_run(root, plan)?;
        match context.stage {
            RunStage::Preflight => Ok(self.services.preflight(root, plan).await?.into_outcome()),
            RunStage::Seed => Ok(self.services.seed(root, plan).await?.into_outcome()),
            RunStage::BaselineSnapshot => Ok(self
                .services
                .capture_baseline(root, plan)
                .await?
                .into_outcome()),
            RunStage::Bootstrap => Ok(self.services.bootstrap(root, plan).await?.into_outcome()),
            RunStage::InitialCheckpoint | RunStage::FinalCheckpoint => Ok(self
                .services
                .checkpoint(root, context.stage, plan)
                .await?
                .into_outcome()),
            RunStage::Warmup | RunStage::Quiet | RunStage::Cdc300 | RunStage::Cdc1000 => {
                let policy = context
                    .policy
                    .context("timed stage is missing its frozen duration policy")?;
                let rate = match context.stage {
                    RunStage::Cdc300 => Some(300),
                    RunStage::Cdc1000 => Some(1_000),
                    _ => None,
                };
                self.run_measured_stage(root, plan, context.stage, policy, rate)
                    .await
            }
            RunStage::RateSearch => self.run_rate_search(root, plan).await,
            RunStage::FailureSequence if plan.mode == RunMode::Isolated => {
                Ok(RuntimeStageEvidence::default().into_outcome())
            }
            RunStage::FailureSequence => {
                let evidence = self.services.failure_sequence(root, plan).await?;
                Ok(StageOutcome {
                    controller_restart_required: true,
                    ..evidence.into_outcome()
                })
            }
            RunStage::Report => Ok(self
                .services
                .report(root, plan, &context.invalidations)
                .await?
                .into_outcome()),
            RunStage::Checksums => Ok(self.services.checksums(root).await?.into_outcome()),
            RunStage::Complete => Ok(RuntimeStageEvidence::default().into_outcome()),
        }
    }

    async fn prepare_isolated_replay(
        &mut self,
        context: StageContext<'_>,
    ) -> Result<IsolatedReplayEvidence> {
        self.services
            .prepare_isolated_replay(context.run_root, context.plan)
            .await
    }
}

impl<S, C> MacComposeRuntime<S, C>
where
    S: R1RuntimeServices,
    C: RuntimeClock,
{
    async fn run_measured_stage(
        &mut self,
        root: &Path,
        plan: &RunPlan,
        stage: RunStage,
        policy: QueryStagePolicy,
        writer_rate: Option<u64>,
    ) -> Result<StageOutcome> {
        if plan.mode == RunMode::Correctness {
            return self
                .run_query_window(root, plan, stage, policy, writer_rate)
                .await;
        }
        let count = u32::try_from(plan.engines.len()).unwrap_or(u32::MAX).max(1);
        let per_engine = QueryStagePolicy {
            scheduled_duration: policy.scheduled_duration / count,
            maximum_duration: policy.maximum_duration / count,
            minimum_samples_per_query: policy.minimum_samples_per_query,
        };
        let mut combined = StageOutcome::default();
        for engine in &plan.engines {
            self.services
                .activate_isolated_engine(root, plan, *engine)
                .await?;
            let mut isolated_plan = plan.clone();
            isolated_plan.engines = vec![*engine];
            let outcome = self
                .run_query_window(root, &isolated_plan, stage, per_engine, writer_rate)
                .await?;
            combined.command_outcomes.extend(outcome.command_outcomes);
            combined.artifact_paths.extend(outcome.artifact_paths);
            combined.query_records.extend(outcome.query_records);
            if !outcome.valid {
                combined.valid = false;
                combined.invalidation = outcome.invalidation;
                break;
            }
        }
        Ok(combined)
    }

    async fn run_rate_search(&mut self, root: &Path, plan: &RunPlan) -> Result<StageOutcome> {
        let mut combined = StageOutcome::default();
        for rate in search_rates(plan.spec.maximum_rate) {
            let policy = QueryStagePolicy {
                scheduled_duration: Duration::from_secs(plan.spec.search_step_secs),
                maximum_duration: Duration::from_secs(plan.spec.search_step_secs).saturating_mul(2),
                minimum_samples_per_query: crate::controller::MINIMUM_QUERY_SAMPLES,
            };
            let effective_policy = if plan.mode == RunMode::Isolated {
                let engines = u32::try_from(plan.engines.len()).unwrap_or(u32::MAX).max(1);
                QueryStagePolicy {
                    scheduled_duration: policy.scheduled_duration.saturating_mul(engines),
                    maximum_duration: policy.maximum_duration.saturating_mul(engines),
                    minimum_samples_per_query: policy.minimum_samples_per_query,
                }
            } else {
                policy
            };
            let outcome = self
                .run_measured_stage(
                    root,
                    plan,
                    RunStage::RateSearch,
                    effective_policy,
                    Some(rate),
                )
                .await?;
            combined.command_outcomes.extend(outcome.command_outcomes);
            combined.artifact_paths.extend(outcome.artifact_paths);
            combined.query_records.extend(outcome.query_records);
            if !outcome.valid {
                combined.valid = false;
                combined.invalidation = outcome.invalidation;
                break;
            }
        }
        Ok(combined)
    }

    async fn run_query_window(
        &mut self,
        root: &Path,
        plan: &RunPlan,
        stage: RunStage,
        policy: QueryStagePolicy,
        writer_rate: Option<u64>,
    ) -> Result<StageOutcome> {
        self.services.set_writer_rate(writer_rate).await?;
        let result = self
            .collect_query_window(root, plan, stage, policy, writer_rate)
            .await;
        let stop_result = self.services.set_writer_rate(None).await;
        match (result, stop_result) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(stop)) => Err(stop.context("stopping writer after timed stage")),
            (Err(error), Err(stop)) => Err(anyhow::anyhow!(
                "timed stage failed: {error:#}; writer stop also failed: {stop:#}"
            )),
        }
    }

    async fn collect_query_window(
        &mut self,
        root: &Path,
        plan: &RunPlan,
        stage: RunStage,
        policy: QueryStagePolicy,
        writer_rate: Option<u64>,
    ) -> Result<StageOutcome> {
        let started = self.clock.elapsed();
        let scheduled_end = started.saturating_add(policy.scheduled_duration);
        let maximum_end = started.saturating_add(policy.maximum_duration);
        let cadence = query_cadence(policy.scheduled_duration);
        let mut next_disk_sample = started;
        let mut next_rate_sample = started;
        let mut records = Vec::new();
        let mut rates = Vec::new();
        let mut ordinal = 0_u64;

        while self.clock.elapsed() < maximum_end {
            let now = self.clock.elapsed();
            if now >= next_disk_sample {
                let disk = self.services.disk_space(root).await?;
                next_disk_sample = now.saturating_add(Duration::from_secs(1));
                if pause_for_free_space(disk.total_bytes, disk.free_bytes) {
                    let invalidation = RunInvalidation::ResourceSafetyGate(format!(
                        "free space {} of {} bytes is below the 15% runtime floor",
                        disk.free_bytes, disk.total_bytes
                    ));
                    let artifacts = persist_timed_evidence(root, stage, &records, &rates)?;
                    return Ok(StageOutcome {
                        artifact_paths: artifacts,
                        valid: false,
                        invalidation: Some(invalidation),
                        query_records: records,
                        ..StageOutcome::default()
                    });
                }
            }

            if let Some(target) = writer_rate {
                if now >= next_rate_sample {
                    let observation = self.services.rate_observation(target).await?;
                    rates.push(observation);
                    next_rate_sample = now.saturating_add(RATE_SAMPLE_INTERVAL);
                    if let Some(invalidation) = rate_search_stop(&rates) {
                        let artifacts = persist_timed_evidence(root, stage, &records, &rates)?;
                        return Ok(StageOutcome {
                            artifact_paths: artifacts,
                            valid: false,
                            invalidation: Some(invalidation),
                            query_records: records,
                            ..StageOutcome::default()
                        });
                    }
                }
            }

            ordinal += 1;
            let checkpoints = self.checkpoints(plan).await?;
            let logical = checkpoints
                .iter()
                .map(|(_, checkpoint)| checkpoint)
                .next()
                .copied()
                .context("query stage has no engine checkpoint")?;
            let schedule_checkpoint = if plan.mode == RunMode::Isolated {
                LogicalCheckpoint {
                    sequence: logical.sequence,
                    source_lsn: 0,
                }
            } else {
                logical
            };
            let schedule = QuerySchedule::new(20_260_901).at(ordinal, schedule_checkpoint);
            let mut digests = BTreeSet::new();
            for engine in &plan.engines {
                let checkpoint = checkpoints
                    .iter()
                    .find_map(|(candidate, checkpoint)| {
                        (*candidate == *engine).then_some(*checkpoint)
                    })
                    .context("engine checkpoint is missing")?;
                let invocation = QueryInvocation {
                    id: schedule.query,
                    parameters: schedule.parameters.clone(),
                    checkpoint,
                    target_lsn: checkpoint.source_lsn,
                };
                let query_started = self.clock.unix_ms();
                match self.services.query(*engine, invocation).await {
                    Ok(result) => {
                        let digest = canonical_digest(&crate::query::QueryResult {
                            columns: result.columns,
                            rows: result.rows,
                        });
                        let failed = result.target_lsn != checkpoint.source_lsn
                            || result.visible_lsn < checkpoint.source_lsn;
                        if !failed {
                            digests.insert(digest.clone());
                        }
                        records.push(StageQueryRecord {
                            query: schedule.query,
                            engine: Some(*engine),
                            target_rows_per_sec: writer_rate,
                            logical_checkpoint: checkpoint.sequence,
                            started_at_unix_ms: query_started,
                            completed_at_unix_ms: Some(self.clock.unix_ms()),
                            target_lsn: checkpoint.source_lsn,
                            visible_lsn: result.visible_lsn,
                            canonical_digest: digest,
                            elapsed_ns: result.elapsed_ns,
                            rows_read: result.rows_read,
                            bytes_read: result.bytes_read,
                            failed,
                            failure: failed
                                .then(|| "engine returned stale or mismatched LSN proof".into()),
                        });
                    }
                    Err(error) => records.push(StageQueryRecord {
                        query: schedule.query,
                        engine: Some(*engine),
                        target_rows_per_sec: writer_rate,
                        logical_checkpoint: checkpoint.sequence,
                        started_at_unix_ms: query_started,
                        completed_at_unix_ms: Some(self.clock.unix_ms()),
                        target_lsn: checkpoint.source_lsn,
                        visible_lsn: 0,
                        canonical_digest: String::new(),
                        elapsed_ns: 0,
                        rows_read: None,
                        bytes_read: None,
                        failed: true,
                        failure: Some(format!("{error:#}")),
                    }),
                }
            }
            if plan.mode == RunMode::Correctness && digests.len() > 1 {
                let artifacts = persist_timed_evidence(root, stage, &records, &rates)?;
                return Ok(StageOutcome {
                    artifact_paths: artifacts,
                    valid: false,
                    invalidation: Some(RunInvalidation::ResultDigestMismatch {
                        query: schedule.query,
                        checkpoint: logical.sequence,
                    }),
                    query_records: records,
                    ..StageOutcome::default()
                });
            }

            if self.clock.elapsed() >= scheduled_end
                && has_minimum_samples(&records, &plan.engines, writer_rate)
            {
                break;
            }
            self.clock.sleep(cadence).await;
        }

        let valid = has_minimum_samples(&records, &plan.engines, writer_rate);
        let artifacts = persist_timed_evidence(root, stage, &records, &rates)?;
        Ok(StageOutcome {
            artifact_paths: artifacts,
            valid,
            invalidation: (!valid).then(|| {
                RunInvalidation::MissingArtifact(format!(
                    "{stage:?} did not record 30 successful Q1-Q5 samples per engine"
                ))
            }),
            query_records: records,
            ..StageOutcome::default()
        })
    }

    async fn checkpoints(
        &mut self,
        plan: &RunPlan,
    ) -> Result<Vec<(EngineKind, LogicalCheckpoint)>> {
        let checkpoints = match plan.mode {
            RunMode::Correctness => {
                // One shared numeric checkpoint for correctness mode.  It is
                // captured exactly once from committed ledger state; sampling
                // per engine would race the active writer and make the two
                // WAL reads differ.
                let Some(first_engine) = plan.engines.first() else {
                    bail!("correctness mode requires at least one engine")
                };
                let checkpoint = self
                    .services
                    .query_checkpoint(plan.mode, *first_engine)
                    .await?;
                plan.engines
                    .iter()
                    .map(|engine| (*engine, checkpoint))
                    .collect()
            }
            RunMode::Isolated => {
                let mut checkpoints = Vec::new();
                for engine in &plan.engines {
                    checkpoints.push((
                        *engine,
                        self.services.query_checkpoint(plan.mode, *engine).await?,
                    ));
                }
                checkpoints
            }
        };
        if let Some(first) = checkpoints.first() {
            match plan.mode {
                // Tripwire, not live protection: correctness clones one
                // captured value, so this can only fire if the capture path
                // changes back to per-engine sampling.
                RunMode::Correctness if checkpoints.iter().any(|(_, c)| *c != first.1) => {
                    bail!("correctness engines did not expose one shared numeric checkpoint")
                }
                RunMode::Isolated
                    if checkpoints
                        .iter()
                        .any(|(_, c)| c.sequence != first.1.sequence) =>
                {
                    bail!("isolated engines did not expose one matching logical checkpoint")
                }
                _ => {}
            }
        }
        Ok(checkpoints)
    }
}

fn query_cadence(scheduled: Duration) -> Duration {
    let nanos = scheduled.as_nanos() / u128::from(QUERY_ATTEMPTS_PER_SCHEDULED_WINDOW);
    Duration::from_nanos(nanos.max(1).min(u128::from(u64::MAX)) as u64)
}

fn has_minimum_samples(
    records: &[StageQueryRecord],
    engines: &[EngineKind],
    rate: Option<u64>,
) -> bool {
    engines.iter().all(|engine| {
        [
            QueryId::Q1,
            QueryId::Q2,
            QueryId::Q3,
            QueryId::Q4,
            QueryId::Q5,
        ]
        .into_iter()
        .all(|query| {
            records
                .iter()
                .filter(|record| {
                    record.engine == Some(*engine)
                        && record.query == query
                        && !record.failed
                        && record.target_rows_per_sec == rate
                })
                .count()
                >= crate::controller::MINIMUM_QUERY_SAMPLES as usize
        })
    })
}

fn persist_timed_evidence(
    root: &Path,
    stage: RunStage,
    records: &[StageQueryRecord],
    rates: &[RateSearchObservation],
) -> Result<Vec<String>> {
    let metrics = root.join("metrics");
    fs::create_dir_all(&metrics)?;
    let rate_suffix = records
        .first()
        .and_then(|record| record.target_rows_per_sec)
        .map(|rate| format!("-{rate}"))
        .unwrap_or_default();
    let engine_suffix = records
        .first()
        .and_then(|first| {
            let engine = first.engine?;
            records
                .iter()
                .all(|record| record.engine == Some(engine))
                .then_some(match engine {
                    EngineKind::Graydb => "-graydb",
                    EngineKind::Clickhouse => "-clickhouse",
                })
        })
        .unwrap_or_default();
    let query_relative = format!("metrics/{stage:?}{rate_suffix}{engine_suffix}-queries.jsonl");
    write_json_lines(&root.join(&query_relative), records)?;
    let mut artifacts = vec![query_relative];
    if !rates.is_empty() {
        let rate_relative =
            format!("metrics/{stage:?}{rate_suffix}{engine_suffix}-rate-observations.jsonl");
        write_json_lines(&root.join(&rate_relative), rates)?;
        artifacts.push(rate_relative);
    }
    Ok(artifacts)
}

fn write_json_lines<T: Serialize>(path: &Path, values: &[T]) -> Result<()> {
    let partial = path.with_extension("jsonl.partial");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&partial)
        .with_context(|| format!("opening {}", partial.display()))?;
    for value in values {
        serde_json::to_writer(&mut file, value)?;
        writeln!(file)?;
    }
    file.sync_all()?;
    fs::rename(&partial, path)?;
    FileSync::sync_parent(path)?;
    Ok(())
}

struct FileSync;

impl FileSync {
    fn sync_parent(path: &Path) -> Result<()> {
        let parent = path.parent().context("artifact path has no parent")?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    }
}

/// Production HTTP engine port.  PostgreSQL/process ownership is supplied by
/// `SystemR1RuntimeServices` below; this wrapper ensures the live query path is
/// exactly the Task 7/8 `EngineAdapter` contract used by fake services.
pub struct HttpEngines {
    graydb: crate::graydb::GrayDbAdapter,
    clickhouse: crate::clickhouse::ClickHouseAdapter,
    graydb_url: String,
    clickhouse_url: String,
    client: reqwest::Client,
}

impl HttpEngines {
    pub fn new(graydb_url: impl Into<String>, clickhouse_url: impl Into<String>) -> Self {
        let graydb_url = graydb_url.into();
        let clickhouse_url = clickhouse_url.into();
        Self {
            graydb: crate::graydb::GrayDbAdapter::new(graydb_url.clone()),
            clickhouse: crate::clickhouse::ClickHouseAdapter::new(clickhouse_url.clone()),
            graydb_url,
            clickhouse_url,
            client: reqwest::Client::new(),
        }
    }

    pub async fn query(
        &self,
        engine: EngineKind,
        invocation: &QueryInvocation,
    ) -> Result<QueryResult> {
        match engine {
            EngineKind::Graydb => self.graydb.query(invocation).await,
            EngineKind::Clickhouse => self.clickhouse.query(invocation).await,
        }
    }

    pub async fn status(&self, engine: EngineKind) -> Result<crate::adapter::EngineStatus> {
        match engine {
            EngineKind::Graydb => self.graydb.status().await,
            EngineKind::Clickhouse => self.clickhouse.status().await,
        }
    }
}

#[async_trait]
impl RuntimeEngines for HttpEngines {
    async fn query(&self, engine: EngineKind, invocation: &QueryInvocation) -> Result<QueryResult> {
        HttpEngines::query(self, engine, invocation).await
    }

    async fn status(&self, engine: EngineKind) -> Result<crate::adapter::EngineStatus> {
        HttpEngines::status(self, engine).await
    }

    async fn wait_visible(
        &self,
        engine: EngineKind,
        target_lsn: u64,
        timeout: Duration,
    ) -> Result<Duration> {
        match engine {
            EngineKind::Graydb => self.graydb.wait_visible(target_lsn, timeout).await,
            EngineKind::Clickhouse => self.clickhouse.wait_visible(target_lsn, timeout).await,
        }
    }

    async fn bootstrap_clickhouse(&self) -> Result<()> {
        crate::clickhouse::ClickHouseSink::new(self.clickhouse_url.clone())
            .execute(include_str!("../../../bench/r1/clickhouse.sql"))
            .await
    }

    async fn load_clickhouse_snapshot(
        &self,
        postgres_host: &str,
        postgres_port: u16,
        postgres_database: &str,
        postgres_user: &str,
        postgres_password: &str,
        source_lsn: u64,
    ) -> Result<()> {
        let existing = self
            .clickhouse
            .select(&format!(
                "SELECT operation_sha256 FROM r1_meta.applied_transactions WHERE source_lsn = {source_lsn}"
            ))
            .await?;
        if let Some(row) = existing.first() {
            let hash = row
                .first()
                .and_then(Option::as_deref)
                .context("ClickHouse initial marker hash was null")?;
            anyhow::ensure!(
                existing.len() == 1 && hash == format!("initial-{source_lsn}"),
                "ClickHouse source LSN {source_lsn} already has a different marker"
            );
            return Ok(());
        }
        let connection = format!(
            "'{}:{}','{}','{{table}}','{}','{}'",
            sql_literal(postgres_host),
            postgres_port,
            sql_literal(postgres_database),
            sql_literal(postgres_user),
            sql_literal(postgres_password)
        );
        let version = (u128::from(source_lsn)) << 32;
        let statements = [
            (
                "r1_tenants_raw",
                "r1.tenants",
                "tenant_id, region, plan, created_at, toString(settings)",
            ),
            (
                "r1_customers_raw",
                "r1.customers",
                "customer_id, tenant_id, segment, email_domain, toString(profile), created_at",
            ),
            (
                "r1_orders_raw",
                "r1.orders",
                "order_id, tenant_id, customer_id, status, channel, amount_cents, created_at, updated_at, toString(attributes)",
            ),
            (
                "r1_order_events_raw",
                "r1.order_events",
                "event_id, order_id, tenant_id, event_type, event_at, toString(metadata)",
            ),
        ];
        let sink = crate::clickhouse::ClickHouseSink::new(self.clickhouse_url.clone());
        for (target, source, columns) in statements {
            let source_fn = format!("postgresql({})", connection.replace("{table}", source));
            sink.execute(&format!(
                "INSERT INTO {target} SELECT {columns}, {source_lsn}, 0, toUInt128({version}), 0 FROM {source_fn}"
            ))
            .await
            .with_context(|| format!("loading ClickHouse snapshot table {source}"))?;
        }
        sink.execute(&format!(
            "INSERT INTO r1_meta.applied_transactions VALUES ('initial-{source_lsn}', {source_lsn}, now())"
        ))
        .await?;
        Ok(())
    }

    async fn replay_count(&self, engine: EngineKind) -> Result<u64> {
        match engine {
            EngineKind::Graydb => {
                let value: serde_json::Value = self
                    .client
                    .get(format!("{}/api/status", self.graydb_url))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                value
                    .get("replay_count")
                    .or_else(|| value.get("replayed_operations"))
                    .and_then(serde_json::Value::as_u64)
                    .context("GrayDB status omitted replay_count")
            }
            EngineKind::Clickhouse => {
                let rows = self
                    .clickhouse
                    .select("SELECT count() FROM r1_meta.applied_transactions")
                    .await?;
                parse_single_u64(&rows, "ClickHouse replay count")
            }
        }
    }

    async fn operation_anomalies(&self, engine: EngineKind) -> Result<(u64, u64, u64)> {
        match engine {
            EngineKind::Graydb => {
                let value: serde_json::Value = self
                    .client
                    .get(format!("{}/api/status", self.graydb_url))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                let required = |name: &str| {
                    value
                        .get(name)
                        .and_then(serde_json::Value::as_u64)
                        .with_context(|| format!("GrayDB status omitted {name}"))
                };
                Ok((
                    required("missing_operations")?,
                    required("duplicate_operations")?,
                    required("out_of_order_operations")?,
                ))
            }
            EngineKind::Clickhouse => {
                let rows = self
                    .clickhouse
                    .select(
                        "SELECT count() - uniqExact(source_lsn), 0, 0 FROM r1_meta.applied_transactions",
                    )
                    .await?;
                let row = rows
                    .first()
                    .context("ClickHouse anomaly query returned no row")?;
                Ok((
                    parse_optional_u64(row.get(1), "ClickHouse missing operations")?,
                    parse_optional_u64(row.first(), "ClickHouse duplicate operations")?,
                    parse_optional_u64(row.get(2), "ClickHouse out-of-order operations")?,
                ))
            }
        }
    }

    async fn start_clickhouse_cdc(
        &self,
        config: &SystemRuntimeConfig,
        run_root: &Path,
    ) -> Result<ClickHouseCdcTask> {
        start_clickhouse_cdc_task(config, &self.clickhouse_url, run_root).await
    }
}

/// The checksum operation is shared by the production services implementation
/// and command-boundary tests.
pub fn write_runtime_checksums(run_root: &Path) -> Result<RuntimeStageEvidence> {
    let path = sha256_tree(run_root)?;
    Ok(RuntimeStageEvidence {
        artifact_paths: vec![path
            .strip_prefix(run_root)
            .unwrap_or(&path)
            .display()
            .to_string()],
        ..RuntimeStageEvidence::default()
    })
}

pub fn baseline_path(run_root: &Path) -> PathBuf {
    run_root.join("baseline/postgres")
}

fn engine_name(engine: EngineKind) -> &'static str {
    match engine {
        EngineKind::Graydb => "graydb",
        EngineKind::Clickhouse => "clickhouse",
    }
}

fn parse_postgres_url(url: &str) -> Result<(String, u16, String, String, String)> {
    let stripped = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .context("R1 PostgreSQL URL must use postgres:// or postgresql://")?;
    let (credentials, address_and_database) = stripped
        .split_once('@')
        .context("R1 PostgreSQL URL must include credentials and a host")?;
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

fn redact_url(url: &str) -> String {
    let Some((scheme, remainder)) = url.split_once("://") else {
        return "<invalid-url>".into();
    };
    let host = remainder
        .split_once('@')
        .map(|(_, host)| host)
        .unwrap_or(remainder);
    format!("{scheme}://<redacted>@{host}")
}

fn sql_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
}

fn parse_optional_u64(value: Option<&Option<String>>, label: &str) -> Result<u64> {
    value
        .and_then(Option::as_deref)
        .with_context(|| format!("{label} was null"))?
        .parse()
        .with_context(|| format!("parsing {label}"))
}

fn parse_single_u64(rows: &[Vec<Option<String>>], label: &str) -> Result<u64> {
    let row = rows
        .first()
        .with_context(|| format!("{label} query returned no row"))?;
    parse_optional_u64(row.first(), label)
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("JSON artifact has no parent")?;
    fs::create_dir_all(parent)?;
    let partial = path.with_extension(format!(
        "{}partial",
        path.extension()
            .and_then(|extension| extension.to_str())
            .map(|extension| format!("{extension}."))
            .unwrap_or_default()
    ));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&partial)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    writeln!(file)?;
    file.sync_all()?;
    fs::rename(&partial, path)?;
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn path_relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
}

fn frozen_git_sha(run_root: &Path) -> Option<String> {
    let raw = fs::read(run_root.join("run-state.json")).ok()?;
    let state: crate::controller::RunState = serde_json::from_slice(&raw).ok()?;
    state.plan?.input_hashes.get("git_sha").cloned()
}

fn runtime_config_record(
    config: &SystemRuntimeConfig,
    plan: &RunPlan,
    active_engine: Option<EngineKind>,
) -> serde_json::Value {
    serde_json::json!({
        "profile": plan.profile,
        "mode": plan.mode,
        "engines": plan.engines,
        "spec": plan.spec,
        "input_hashes": plan.input_hashes,
        "compose_file": config.compose_file,
        "compose_project": config.project_name,
        "postgres_endpoint": format!("{}:{}/{}", config.postgres_host, config.postgres_port, config.postgres_database),
        "active_isolated_engine": active_engine,
        "seed": config.seed,
    })
}

fn manifest_initial_lsn(run_root: &Path) -> Result<u64> {
    let manifest: crate::manifest::DatasetManifest =
        serde_json::from_slice(&fs::read(run_root.join("dataset-manifest.json"))?)?;
    let lsn = manifest
        .initial_lsn
        .context("dataset manifest omitted initial PostgreSQL LSN")?;
    graydb_ingest::repl::parse_lsn(&lsn)
}

async fn postgres_lsn(client: &Client) -> Result<u64> {
    let lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    graydb_ingest::repl::parse_lsn(&lsn)
}

fn committed_rows(run_root: &Path) -> Result<u64> {
    let ledger = crate::ledger::CommittedLedger::resume(run_root)?;
    let intents = crate::ledger::IntentLog::create(run_root)?.read_all()?;
    let plans = intents
        .into_iter()
        .map(|plan| (plan.sequence, plan))
        .collect::<BTreeMap<_, _>>();
    ledger.entries().iter().try_fold(0_u64, |rows, entry| {
        let plan = plans.get(&entry.sequence).with_context(|| {
            format!(
                "committed sequence {} has no durable intent",
                entry.sequence
            )
        })?;
        anyhow::ensure!(
            plan.operation_sha256 == entry.operation_sha256,
            "committed sequence {} intent hash mismatch",
            entry.sequence
        );
        Ok(rows.saturating_add(plan.operations.len() as u64))
    })
}

fn verify_checksum_file(root: &Path, checksum_path: &Path) -> Result<()> {
    use sha2::{Digest, Sha256};
    let raw = fs::read_to_string(checksum_path)?;
    for (line_number, line) in raw.lines().enumerate() {
        let (expected, relative) = line.split_once("  ").with_context(|| {
            format!(
                "invalid checksum line {} in {}",
                line_number + 1,
                checksum_path.display()
            )
        })?;
        let path = root.join(relative);
        anyhow::ensure!(
            path.is_file(),
            "checksummed artifact is missing: {}",
            path.display()
        );
        let actual = format!("{:x}", Sha256::digest(fs::read(&path)?));
        anyhow::ensure!(
            actual == expected,
            "checksum mismatch for {}: expected {}, got {}",
            path.display(),
            expected,
            actual
        );
    }
    Ok(())
}

fn runtime_compose_request(
    config: &SystemRuntimeConfig,
    run_root: &Path,
    data_root: &Path,
    args: impl IntoIterator<Item = impl Into<String>>,
) -> RuntimeProcessRequest {
    let mut command_args = vec![
        "compose".into(),
        "--project-name".into(),
        config.project_name.clone(),
        "--file".into(),
        config.compose_file.display().to_string(),
    ];
    command_args.extend(args.into_iter().map(Into::into));
    RuntimeProcessRequest {
        program: "docker".into(),
        args: command_args,
        cwd: Some(config.repository_root.clone()),
        environment: BTreeMap::from([
            ("R1_DATA_ROOT".into(), data_root.display().to_string()),
            (
                "R1_GIT_SHA".into(),
                frozen_git_sha(run_root).unwrap_or_else(|| "unrecorded".into()),
            ),
        ]),
    }
}

fn run_process_checked(
    process: &dyn RuntimeProcess,
    request: RuntimeProcessRequest,
) -> Result<CommandOutcome> {
    let outcome = process.run(&request)?;
    anyhow::ensure!(
        outcome.is_success(),
        "{} {:?} failed with {:?}: {}",
        outcome.program,
        outcome.args,
        outcome.exit_code,
        outcome.stderr.trim()
    );
    Ok(outcome)
}

async fn connect_with_timeout(
    connector: &dyn RuntimePostgresConnector,
    database_url: &str,
) -> Result<Client> {
    let started = Instant::now();
    loop {
        match connector.connect(database_url).await {
            Ok(client) => return Ok(client),
            Err(error) if started.elapsed() < Duration::from_secs(60) => {
                tracing::debug!(%error, "waiting for restored PostgreSQL");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(error) => {
                return Err(error.context("restored PostgreSQL was not ready in 60 seconds"))
            }
        }
    }
}

async fn prepare_system_isolated_replay(
    config: &SystemRuntimeConfig,
    process: &dyn RuntimeProcess,
    connector: &dyn RuntimePostgresConnector,
    run_root: &Path,
    plan: &RunPlan,
) -> Result<IsolatedReplayEvidence> {
    anyhow::ensure!(
        plan.engines.len() == 2
            && plan.engines.contains(&EngineKind::Graydb)
            && plan.engines.contains(&EngineKind::Clickhouse),
        "isolated comparison requires exactly GrayDB and ClickHouse"
    );
    let ledger = crate::ledger::CommittedLedger::resume(run_root)?;
    anyhow::ensure!(
        !ledger.entries().is_empty(),
        "isolated mode requires a non-empty committed workload ledger before replay"
    );
    let intents = crate::ledger::IntentLog::create(run_root)?.read_all()?;
    let by_sequence = intents
        .into_iter()
        .map(|intent| (intent.sequence, intent))
        .collect::<BTreeMap<_, _>>();
    let committed = ledger
        .entries()
        .iter()
        .map(|entry| {
            let intent = by_sequence.get(&entry.sequence).with_context(|| {
                format!("ledger sequence {} has no durable intent", entry.sequence)
            })?;
            anyhow::ensure!(
                intent.operation_sha256 == entry.operation_sha256,
                "ledger sequence {} differs from its durable intent hash",
                entry.sequence
            );
            Ok((intent.clone(), entry.clone()))
        })
        .collect::<Result<Vec<_>>>()?;

    let baseline = crate::controller::BaselineSnapshot {
        postgres_dir: baseline_path(run_root),
        checksum_path: baseline_path(run_root).join("SHA256SUMS"),
    };
    verify_checksum_file(&baseline.postgres_dir, &baseline.checksum_path)?;
    run_process_checked(
        process,
        runtime_compose_request(
            config,
            run_root,
            &SystemR1RuntimeServices::<SystemRuntimeProcess, TokioPostgresConnector, HttpEngines>::service_root(run_root),
            ["down", "--remove-orphans"],
        ),
    )?;

    for engine in [EngineKind::Graydb, EngineKind::Clickhouse] {
        baseline.restore_isolated(run_root, engine)?;
    }

    let mut workload_hashes = Vec::new();
    let mut replay_maps = Vec::new();
    let mut logical_checkpoints = Vec::new();
    for engine in [EngineKind::Graydb, EngineKind::Clickhouse] {
        let isolated_root = run_root.join("isolated").join(engine_name(engine));
        run_process_checked(
            process,
            runtime_compose_request(
                config,
                run_root,
                &isolated_root,
                ["up", "--detach", "--wait", "postgres"],
            ),
        )?;
        let replay_result = async {
            let mut client = connect_with_timeout(connector, &config.postgres_url).await?;
            let mut replay_entries = Vec::with_capacity(committed.len());
            for (intent, entry) in &committed {
                execute_replay_transaction(&mut client, intent).await?;
                replay_entries.push((intent.clone(), entry.clone(), postgres_lsn(&client).await?));
            }
            Ok::<_, anyhow::Error>(replay_entries)
        }
        .await;
        let down_result = run_process_checked(
            process,
            runtime_compose_request(
                config,
                run_root,
                &isolated_root,
                ["down", "--remove-orphans"],
            ),
        );
        let replay_entries = match (replay_result, down_result) {
            (Ok(entries), Ok(_)) => entries,
            (Err(error), Ok(_)) => return Err(error),
            (Ok(_), Err(error)) => return Err(error.context("stopping restored PostgreSQL")),
            (Err(replay), Err(stop)) => {
                bail!("isolated replay failed: {replay:#}; restored PostgreSQL stop also failed: {stop:#}")
            }
        };
        let map = crate::replication::ReplayMap::create(&isolated_root)?;
        let mut replayer = crate::replication::WorkloadReplayer::new(map);
        replayer.replay(&replay_entries)?;
        let map = replayer.into_replay_map();
        workload_hashes.push(
            replay_entries
                .iter()
                .map(|(intent, _, _)| intent.operation_sha256.clone())
                .collect(),
        );
        logical_checkpoints.push(
            map.entries()
                .last()
                .context("isolated replay produced no map entry")?
                .logical_sequence,
        );
        replay_maps.push(map.entries().to_vec());
    }
    let evidence = IsolatedReplayEvidence {
        workload_hashes,
        replay_maps,
        logical_checkpoints,
    };
    evidence.validate()?;
    Ok(evidence)
}

async fn execute_replay_transaction(
    client: &mut Client,
    plan: &crate::workload::TransactionPlan,
) -> Result<()> {
    use crate::workload::Operation;
    let transaction = client.transaction().await?;
    for operation in &plan.operations {
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
    }
    transaction
        .execute(
            "INSERT INTO r1_control.tx_marker (sequence, operation_sha256) VALUES ($1,$2)",
            &[&(plan.sequence as i64), &plan.operation_sha256],
        )
        .await?;
    transaction.commit().await?;
    Ok(())
}

async fn activate_system_isolated_engine<P, C, E>(
    services: &mut SystemR1RuntimeServices<P, C, E>,
    run_root: &Path,
    plan: &RunPlan,
    engine: EngineKind,
) -> Result<()>
where
    P: RuntimeProcess,
    C: RuntimePostgresConnector,
    E: RuntimeEngines,
{
    anyhow::ensure!(
        plan.mode == RunMode::Isolated,
        "engine activation is isolated-only"
    );
    if services.active_isolated_engine == Some(engine) {
        return Ok(());
    }
    if let Some(cdc) = services.clickhouse_cdc.take() {
        cdc.stop().await?;
    }
    if let Some(previous) = services.active_isolated_engine {
        let previous_root = run_root.join("isolated").join(engine_name(previous));
        services.run_checked(services.compose_request(
            run_root,
            &previous_root,
            ["down", "--remove-orphans"],
        ))?;
    }
    let isolated_root = run_root.join("isolated").join(engine_name(engine));
    let selected = engine_name(engine);
    services.run_checked(services.compose_request(
        run_root,
        &isolated_root,
        ["up", "--detach", "--wait", "postgres", selected],
    ))?;
    services.wait_postgres().await?;
    let map = crate::replication::ReplayMap::resume(&isolated_root)?;
    let checkpoint = map
        .entries()
        .last()
        .context("isolated replay map contains no checkpoint")?;
    if engine == EngineKind::Clickhouse {
        services.engines.bootstrap_clickhouse().await?;
        services
            .engines
            .load_clickhouse_snapshot(
                "postgres",
                5432,
                &services.config.postgres_database,
                &services.config.postgres_user,
                &services.config.postgres_password,
                checkpoint.replay_source_lsn,
            )
            .await?;
        services.clickhouse_cdc = Some(
            services
                .engines
                .start_clickhouse_cdc(&services.config, run_root)
                .await?,
        );
    }
    services
        .engines
        .wait_visible(
            engine,
            checkpoint.replay_source_lsn,
            Duration::from_secs(30 * 60),
        )
        .await?;
    services.active_isolated_engine = Some(engine);
    write_json_atomic(
        &run_root.join("configs/runtime.json"),
        &runtime_config_record(&services.config, plan, Some(engine)),
    )?;
    Ok(())
}

struct RuntimeWriterControl<'a, C>
where
    C: RuntimePostgresConnector,
{
    writer: &'a Arc<tokio::sync::Mutex<WriterCoordinator>>,
    run_root: &'a Path,
    config: &'a SystemRuntimeConfig,
    connector: &'a C,
    paused_rate: tokio::sync::Mutex<Option<u64>>,
}

#[async_trait]
impl<C> crate::oracle::WriterControl for RuntimeWriterControl<'_, C>
where
    C: RuntimePostgresConnector,
{
    async fn pause(&self) -> Result<()> {
        let mut writer = self.writer.lock().await;
        let rate = writer.active_rate();
        writer.stop().await?;
        *self.paused_rate.lock().await = rate;
        Ok(())
    }

    async fn drain(&self) -> Result<()> {
        anyhow::ensure!(
            self.writer.lock().await.active_rate().is_none(),
            "writer still has an in-flight task after pause"
        );
        Ok(())
    }

    async fn resume(&self) -> Result<()> {
        if let Some(rate) = self.paused_rate.lock().await.take() {
            self.writer
                .lock()
                .await
                .start(rate, self.run_root, self.config, self.connector)
                .await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RuntimeCheckpointBundle {
    mode: RunMode,
    checkpoints: Vec<crate::oracle::VerifiedCheckpoint>,
    verdict: crate::oracle::CorrectnessVerdict,
}

async fn capture_runtime_checkpoint<C, E>(
    connector: &C,
    engines: &E,
    writer: &Arc<tokio::sync::Mutex<WriterCoordinator>>,
    run_root: &Path,
    config: &SystemRuntimeConfig,
    plan: &RunPlan,
) -> Result<RuntimeCheckpointBundle>
where
    C: RuntimePostgresConnector,
    E: RuntimeEngines,
{
    let writer_control = RuntimeWriterControl {
        writer,
        run_root,
        config,
        connector,
        paused_rate: tokio::sync::Mutex::new(None),
    };
    let mut client = connector.connect(&config.postgres_url).await?;
    let ledger = crate::ledger::CommittedLedger::resume(run_root)?;
    let sequence = ledger
        .entries()
        .last()
        .map(|entry| entry.sequence)
        .unwrap_or(0);
    let provisional = LogicalCheckpoint {
        sequence,
        source_lsn: if plan.mode == RunMode::Isolated {
            0
        } else {
            postgres_lsn(&client).await?
        },
    };
    let params = crate::query::QueryParameters::for_checkpoint(config.seed, provisional);
    let captured =
        crate::oracle::PostgresCheckpoint::capture_source(&mut client, &writer_control, &params)
            .await?;
    let checkpoint = LogicalCheckpoint {
        sequence,
        source_lsn: captured.source_lsn,
    };
    let mut differences = Vec::new();
    let mut engine_evidence = Vec::new();
    for engine in &plan.engines {
        engines
            .wait_visible(*engine, captured.source_lsn, Duration::from_secs(30 * 60))
            .await?;
        let mut query_digests = BTreeMap::new();
        for (name, query) in crate::oracle::QUERIES {
            let result = engines
                .query(
                    *engine,
                    &QueryInvocation {
                        id: query,
                        parameters: params.clone(),
                        checkpoint,
                        target_lsn: captured.source_lsn,
                    },
                )
                .await?;
            let digest = canonical_digest(&crate::query::QueryResult {
                columns: result.columns,
                rows: result.rows,
            });
            if result.target_lsn != captured.source_lsn || result.visible_lsn < captured.source_lsn
            {
                differences.push(crate::oracle::RowDifference {
                    table: format!("engine:{}", engine_name(*engine)),
                    primary_key: query as u64,
                    expected_version: captured.source_lsn,
                    actual_version: result.visible_lsn,
                    target_checkpoint: captured.source_lsn,
                    detail: format!("{name} returned stale or mismatched LSN proof"),
                });
            }
            if captured.query_digests.get(name) != Some(&digest) {
                differences.push(crate::oracle::RowDifference {
                    table: format!("engine:{}", engine_name(*engine)),
                    primary_key: query as u64,
                    expected_version: captured.source_lsn,
                    actual_version: result.visible_lsn,
                    target_checkpoint: captured.source_lsn,
                    detail: format!("{name} canonical digest mismatch"),
                });
            }
            query_digests.insert(name.to_owned(), digest);
        }
        engine_evidence.push(crate::oracle::EngineCheckpointEvidence {
            engine: engine_name(*engine).into(),
            query_digests,
            samples: Vec::new(),
        });
    }
    let verdict = crate::oracle::CorrectnessVerdict {
        passed: differences.is_empty(),
        invalidations: differences
            .iter()
            .map(|difference| RunInvalidation::ResultDigestMismatch {
                query: match difference.primary_key {
                    0 => QueryId::Q1,
                    1 => QueryId::Q2,
                    2 => QueryId::Q3,
                    3 => QueryId::Q4,
                    _ => QueryId::Q5,
                },
                checkpoint: difference.target_checkpoint,
            })
            .collect(),
        differences,
    };
    Ok(RuntimeCheckpointBundle {
        mode: plan.mode,
        checkpoints: vec![crate::oracle::VerifiedCheckpoint {
            checkpoint: captured,
            engines: engine_evidence,
            verdict: verdict.clone(),
        }],
        verdict,
    })
}

struct SystemFailureWorkload<C, E>
where
    C: RuntimePostgresConnector,
    E: RuntimeEngines,
{
    connector: Arc<C>,
    engines: Arc<E>,
    writer: Arc<tokio::sync::Mutex<WriterCoordinator>>,
    config: SystemRuntimeConfig,
    run_root: PathBuf,
    plan: RunPlan,
}

#[async_trait]
impl<C, E> crate::failure::FailureWorkload for SystemFailureWorkload<C, E>
where
    C: RuntimePostgresConnector + 'static,
    E: RuntimeEngines + 'static,
{
    async fn wait_for_steady_state(
        &self,
        target_rows_per_second: u64,
        duration: Duration,
    ) -> Result<()> {
        let before = committed_rows(&self.run_root)?;
        self.writer
            .lock()
            .await
            .start(
                target_rows_per_second,
                &self.run_root,
                &self.config,
                self.connector.as_ref(),
            )
            .await?;
        tokio::time::sleep(duration).await;
        let after = committed_rows(&self.run_root)?;
        let required = target_rows_per_second
            .saturating_mul(duration.as_secs())
            .saturating_mul(95)
            / 100;
        anyhow::ensure!(
            after.saturating_sub(before) >= required,
            "failure steady state achieved {} changed rows, required at least {}",
            after.saturating_sub(before),
            required
        );
        Ok(())
    }

    async fn source_rows_written(&self) -> Result<u64> {
        committed_rows(&self.run_root)
    }

    async fn wait_caught_up(&self, engine: EngineKind, timeout: Duration) -> Result<Duration> {
        self.engines
            .wait_visible(engine, self.source_lsn().await?, timeout)
            .await
    }

    async fn validate(&self, engine: EngineKind) -> Result<crate::oracle::CorrectnessVerdict> {
        let source = self.source_lsn().await?;
        self.validate_lsn_range(engine, 0, source).await
    }

    async fn stop_writer(&self) -> Result<()> {
        self.writer.lock().await.stop().await
    }

    async fn resume_writer(&self) -> Result<()> {
        self.writer
            .lock()
            .await
            .start(
                crate::failure::FAILURE_ROWS_PER_SECOND,
                &self.run_root,
                &self.config,
                self.connector.as_ref(),
            )
            .await
    }

    async fn source_lsn(&self) -> Result<u64> {
        let client = self.connector.connect(&self.config.postgres_url).await?;
        postgres_lsn(&client).await
    }

    async fn received_lsn(&self, engine: EngineKind) -> Result<u64> {
        self.engines
            .status(engine)
            .await?
            .applied_lsn
            .with_context(|| format!("{engine:?} status omitted received/applied LSN"))
    }

    async fn applied_lsn(&self, engine: EngineKind) -> Result<u64> {
        self.engines
            .status(engine)
            .await?
            .applied_lsn
            .with_context(|| format!("{engine:?} status omitted applied LSN"))
    }

    async fn replay_count(&self, engine: EngineKind) -> Result<u64> {
        self.engines.replay_count(engine).await
    }

    async fn missing_operations(&self, engine: EngineKind, _: u64, _: u64) -> Result<u64> {
        Ok(self.engines.operation_anomalies(engine).await?.0)
    }

    async fn duplicate_operations(&self, engine: EngineKind, _: u64, _: u64) -> Result<u64> {
        Ok(self.engines.operation_anomalies(engine).await?.1)
    }

    async fn out_of_order_operations(&self, engine: EngineKind, _: u64, _: u64) -> Result<u64> {
        Ok(self.engines.operation_anomalies(engine).await?.2)
    }

    async fn validate_lsn_range(
        &self,
        engine: EngineKind,
        from: u64,
        through: u64,
    ) -> Result<crate::oracle::CorrectnessVerdict> {
        anyhow::ensure!(through >= from, "failure LSN range is reversed");
        let mut engine_plan = self.plan.clone();
        engine_plan.engines = vec![engine];
        let bundle = capture_runtime_checkpoint(
            self.connector.as_ref(),
            self.engines.as_ref(),
            &self.writer,
            &self.run_root,
            &self.config,
            &engine_plan,
        )
        .await?;
        let captured = bundle
            .checkpoints
            .first()
            .context("failure checkpoint produced no evidence")?
            .checkpoint
            .source_lsn;
        anyhow::ensure!(
            captured >= through,
            "failure checkpoint LSN {captured} does not cover post-recovery LSN {through}"
        );
        Ok(bundle.verdict)
    }
}

async fn run_system_failure_sequence<P, C, E>(
    services: &mut SystemR1RuntimeServices<P, C, E>,
    run_root: &Path,
    plan: &RunPlan,
) -> Result<RuntimeStageEvidence>
where
    P: RuntimeProcess + 'static,
    C: RuntimePostgresConnector + 'static,
    E: RuntimeEngines + 'static,
{
    anyhow::ensure!(
        plan.mode == RunMode::Correctness
            && plan.engines.contains(&EngineKind::Graydb)
            && plan.engines.contains(&EngineKind::Clickhouse),
        "failure sequence requires a two-engine correctness run"
    );
    let workload = Arc::new(SystemFailureWorkload {
        connector: services.connector.clone(),
        engines: services.engines.clone(),
        writer: services.writer.clone(),
        config: services.config.clone(),
        run_root: run_root.to_path_buf(),
        plan: plan.clone(),
    });
    let compose = Arc::new(crate::failure::SystemComposeControl::new(
        services.config.compose_file.clone(),
        services.config.project_name.clone(),
        SystemR1RuntimeServices::<P, C, E>::service_root(run_root),
    ));
    let runner = crate::failure::FailureRunner::new(compose, workload);
    let project = &services.config.project_name;
    let result = runner
        .run_failure_sequence([
            crate::failure::CdcEndpoint {
                network: format!("{project}_default"),
                endpoint: format!("{project}-graydb-1"),
                engine: EngineKind::Graydb,
            },
            crate::failure::CdcEndpoint {
                network: format!("{project}_default"),
                endpoint: format!("{project}-clickhouse-1"),
                engine: EngineKind::Clickhouse,
            },
        ])
        .await;
    let stop = services.writer.lock().await.stop().await;
    let result = match (result, stop) {
        (Ok(result), Ok(())) => result,
        (Err(error), Ok(())) => return Err(error),
        (Ok(_), Err(error)) => return Err(error.context("stopping writer after failure sequence")),
        (Err(sequence), Err(stop)) => {
            bail!("failure sequence failed: {sequence:#}; writer stop also failed: {stop:#}")
        }
    };
    let path = run_root.join("failure-events/sequence.json");
    write_json_atomic(&path, &result)?;
    let command_outcomes = result
        .engine_failures
        .iter()
        .flat_map(|failure| failure.commands.clone())
        .chain(result.cdc_interruptions.iter().flatten().cloned())
        .collect();
    Ok(RuntimeStageEvidence {
        command_outcomes,
        artifact_paths: vec![path_relative(run_root, &path)],
    })
}

const CLICKHOUSE_CDC_SLOT: &str = "graydb_r1_clickhouse_cdc_slot";

async fn start_clickhouse_cdc_task(
    config: &SystemRuntimeConfig,
    clickhouse_url: &str,
    run_root: &Path,
) -> Result<ClickHouseCdcTask> {
    use graydb_ingest::repl::ReplClient;
    let admin = TokioPostgresConnector.connect(&config.postgres_url).await?;
    admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1 AND NOT active",
            &[&CLICKHOUSE_CDC_SLOT],
        )
        .await
        .context("dropping an inactive prior ClickHouse CDC slot")?;
    let mut replication = ReplClient::connect(
        &config.postgres_host,
        config.postgres_port,
        &config.postgres_user,
        &config.postgres_password,
        &config.postgres_database,
    )
    .await?;
    let snapshot = replication
        .create_slot_with_snapshot(CLICKHOUSE_CDC_SLOT)
        .await?;
    let initial_lsn = graydb_ingest::repl::parse_lsn(&snapshot.consistent_point)?;
    replication
        .start_replication(CLICKHOUSE_CDC_SLOT, "graydb_r1_pub", initial_lsn)
        .await?;
    let config = config.clone();
    let clickhouse_url = clickhouse_url.to_owned();
    let run_root = run_root.to_path_buf();
    let (stop, mut stop_rx) = watch::channel(false);
    let join = tokio::spawn(async move {
        use graydb_ingest::repl::{ReplClient, ReplMsg};
        use graydb_log::{Frame, FrameLog};
        let mut frame_log =
            FrameLog::create(&run_root.join("clickhouse-frame-log"), 64 << 20).await?;
        let mut cdc = crate::clickhouse::ClickHouseCdcAdapter::new(clickhouse_url.clone());
        let mut applied_lsn = initial_lsn;
        let mut reconnect_started = None;
        loop {
            let message = tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        replication.send_standby_status(applied_lsn, false).await.ok();
                        replication.close().await.ok();
                        return Ok(());
                    }
                    continue;
                }
                message = replication.next_replication_message() => message,
            };
            let message = match message {
                Ok(message) => {
                    reconnect_started = None;
                    message
                }
                Err(error) => {
                    tracing::warn!(%error, "ClickHouse CDC source connection interrupted");
                    cdc = crate::clickhouse::ClickHouseCdcAdapter::new(clickhouse_url.clone());
                    let reconnect = *reconnect_started.get_or_insert_with(Instant::now);
                    if reconnect.elapsed() >= crate::failure::CATCHUP_TIMEOUT {
                        return Err(
                            error.context("ClickHouse CDC could not reconnect within 30 minutes")
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    match ReplClient::connect(
                        &config.postgres_host,
                        config.postgres_port,
                        &config.postgres_user,
                        &config.postgres_password,
                        &config.postgres_database,
                    )
                    .await
                    {
                        Ok(mut candidate) => {
                            if candidate
                                .start_replication(
                                    CLICKHOUSE_CDC_SLOT,
                                    "graydb_r1_pub",
                                    applied_lsn,
                                )
                                .await
                                .is_ok()
                            {
                                replication = candidate;
                            }
                        }
                        Err(reconnect_error) => {
                            tracing::debug!(%reconnect_error, "ClickHouse CDC reconnect pending");
                        }
                    }
                    continue;
                }
            };
            match message {
                ReplMsg::XLogData { wal_start, payload } => {
                    let commit_lsn = pgoutput_commit_lsn(&payload);
                    let lsn_end = commit_lsn.unwrap_or(wal_start);
                    let sequence = frame_log
                        .append(wal_start, lsn_end, commit_lsn.is_some(), payload.clone())
                        .await?;
                    let hash = match commit_lsn {
                        Some(lsn) => wait_for_ledger_hash(&run_root, lsn).await?,
                        None => String::new(),
                    };
                    let frame = Frame {
                        seq: sequence,
                        lsn_start: wal_start,
                        lsn_end,
                        txn_complete: commit_lsn.is_some(),
                        payload,
                    };
                    match cdc.apply_frames(&mut replication, &hash, &[frame]).await {
                        Ok(Some(_)) => applied_lsn = lsn_end,
                        Ok(None) => {}
                        Err(error) => {
                            tracing::warn!(%error, "ClickHouse CDC apply interrupted; reconnecting from last applied LSN");
                            cdc = crate::clickhouse::ClickHouseCdcAdapter::new(
                                clickhouse_url.clone(),
                            );
                            replication.close().await.ok();
                            replication = ReplClient::connect(
                                &config.postgres_host,
                                config.postgres_port,
                                &config.postgres_user,
                                &config.postgres_password,
                                &config.postgres_database,
                            )
                            .await?;
                            replication
                                .start_replication(
                                    CLICKHOUSE_CDC_SLOT,
                                    "graydb_r1_pub",
                                    applied_lsn,
                                )
                                .await?;
                        }
                    }
                }
                ReplMsg::Keepalive {
                    reply_requested: true,
                    ..
                } => replication.send_standby_status(applied_lsn, false).await?,
                ReplMsg::Keepalive { .. } => {}
            }
        }
    });
    Ok(ClickHouseCdcTask { stop, join })
}

fn pgoutput_commit_lsn(payload: &[u8]) -> Option<u64> {
    (payload.len() >= 26 && payload.first() == Some(&b'C'))
        .then(|| u64::from_be_bytes(payload[10..18].try_into().expect("checked commit frame")))
}

async fn wait_for_ledger_hash(run_root: &Path, source_lsn: u64) -> Result<String> {
    let started = Instant::now();
    loop {
        let ledger = crate::ledger::CommittedLedger::resume(run_root)?;
        if let Some(entry) = ledger
            .entries()
            .iter()
            .find(|entry| entry.source_lsn == source_lsn)
        {
            return Ok(entry.operation_sha256.clone());
        }
        anyhow::ensure!(
            started.elapsed() < Duration::from_secs(30),
            "ClickHouse CDC commit LSN {source_lsn} had no committed ledger hash after 30 seconds"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn write_system_report(
    run_root: &Path,
    plan: &RunPlan,
    invalidations: &[RunInvalidation],
) -> Result<RuntimeStageEvidence> {
    let state: crate::controller::RunState =
        serde_json::from_slice(&fs::read(run_root.join("run-state.json"))?)
            .context("reading durable run state for report")?;
    let mut reasons = invalidations.to_vec();
    for required in RunStage::ordered()
        .iter()
        .copied()
        .take_while(|stage| *stage != RunStage::Report)
    {
        if !state
            .stages
            .get(&required)
            .is_some_and(|record| record.completed && record.valid)
        {
            let reason = RunInvalidation::MissingArtifact(format!(
                "required stage {required:?} has no valid durable completion"
            ));
            if !reasons.contains(&reason) {
                reasons.push(reason);
            }
        }
    }
    let manifest = fs::read(run_root.join("dataset-manifest.json"))
        .ok()
        .filter(|bytes| !bytes.is_empty())
        .and_then(|bytes| serde_json::from_slice::<crate::manifest::DatasetManifest>(&bytes).ok());
    if manifest.is_none() {
        reasons.push(RunInvalidation::MissingArtifact(
            "dataset-manifest.json is absent or invalid".into(),
        ));
    }
    let final_checkpoint = fs::read(run_root.join("correctness/FinalCheckpoint.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<RuntimeCheckpointBundle>(&bytes).ok());
    if final_checkpoint.is_none() {
        reasons.push(RunInvalidation::MissingArtifact(
            "final correctness checkpoint is absent or invalid".into(),
        ));
    }
    let mut query_series = BTreeMap::<String, crate::metrics::LatencySeries>::new();
    for (stage, record) in &state.stages {
        for sample in record.query_records.iter().filter(|sample| !sample.failed) {
            let engine = sample.engine.map(engine_name).unwrap_or("unknown");
            let rate = sample
                .target_rows_per_sec
                .map(|rate| rate.to_string())
                .unwrap_or_else(|| "none".into());
            let key = format!("{stage:?}/{engine}/{:?}/rate-{rate}", sample.query);
            query_series
                .entry(key)
                .or_insert(crate::metrics::LatencySeries::new(3)?)
                .record_micros(
                    u64::try_from(sample.elapsed_ns / 1_000)
                        .unwrap_or(u64::MAX)
                        .max(1),
                )?;
        }
    }
    let query_latency = query_series
        .into_iter()
        .map(|(key, series)| (key, series.summary()))
        .collect::<BTreeMap<_, _>>();
    if reasons.is_empty() && query_latency.is_empty() {
        reasons.push(RunInvalidation::MissingArtifact(
            "no successful timed query samples were recorded".into(),
        ));
    }
    let rates = load_rate_observations(run_root)?;
    let latest_rate = rates.last().cloned().unwrap_or(RateSearchObservation {
        target_rows_per_sec: 0,
        achieved_rows_per_sec: 0,
        freshness_p99_ms: 0,
        backlog_bytes: 0,
        backlog_growing: false,
        correctness_passed: false,
        resource_gate: None,
    });
    let total_time_ns = state
        .stages
        .values()
        .filter_map(|stage| {
            Some(
                stage
                    .ended_at_unix_ms?
                    .saturating_sub(stage.started_at_unix_ms?),
            )
        })
        .sum::<u128>()
        .saturating_mul(1_000_000);
    let dataset = manifest
        .as_ref()
        .map(|manifest| crate::report::DatasetEvidence {
            bytes: manifest.published_table_bytes,
            rows: manifest.tables.values().map(|table| table.rows).sum(),
        })
        .unwrap_or_default();
    let correctness = final_checkpoint
        .as_ref()
        .map(|bundle| bundle.verdict.clone());
    if correctness.as_ref().is_some_and(|verdict| !verdict.passed) && reasons.is_empty() {
        reasons.push(RunInvalidation::ResultDigestMismatch {
            query: QueryId::Q1,
            checkpoint: 0,
        });
    }
    let valid = reasons.is_empty();
    let result = crate::report::RunResult {
        benchmark_id: "R1-P1-v1".into(),
        profile: Some(plan.profile),
        valid,
        total_time_ns,
        dataset,
        correctness,
        invalidations: reasons,
        query_latency,
        freshness: crate::report::FreshnessEvidence {
            // ponytail: p50/p95 need a per-query freshness series the stage
            // mechanics do not record yet; p99 and sample count are real.
            // Record the series in timed stages before filling these in.
            p50_ms: 0,
            p95_ms: 0,
            p99_ms: latest_rate.freshness_p99_ms,
            samples: rates.len() as u64,
        },
        source_rate: crate::report::SourceRateEvidence {
            target_rows_per_sec: latest_rate.target_rows_per_sec,
            achieved_rows_per_sec: latest_rate.achieved_rows_per_sec,
        },
        artifact_paths: vec![
            "run-state.json".into(),
            "dataset-manifest.json".into(),
            "correctness/FinalCheckpoint.json".into(),
        ],
        ..crate::report::RunResult::default()
    };
    crate::report::ReportWriter::write(run_root, &result)?;
    Ok(RuntimeStageEvidence {
        artifact_paths: vec![
            "result.json".into(),
            "result.md".into(),
            "aws-capacity-request.json".into(),
        ],
        ..RuntimeStageEvidence::default()
    })
}

fn load_rate_observations(run_root: &Path) -> Result<Vec<RateSearchObservation>> {
    let metrics = run_root.join("metrics");
    if !metrics.is_dir() {
        return Ok(Vec::new());
    }
    let mut paths = fs::read_dir(metrics)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains("rate-observations") && name.ends_with(".jsonl"))
        })
        .collect::<Vec<_>>();
    paths.sort();
    let mut observations = Vec::new();
    for path in paths {
        for line in fs::read_to_string(path)?
            .lines()
            .filter(|line| !line.is_empty())
        {
            observations.push(serde_json::from_str(line)?);
        }
    }
    Ok(observations)
}
