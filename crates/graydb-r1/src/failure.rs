//! Planned failure orchestration.  All external process execution crosses the
//! `ComposeControl::invoke` boundary as an executable plus argument vector.

use crate::contracts::EngineKind;
use crate::controller::CommandOutcome;
use crate::oracle::CorrectnessVerdict;
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

pub const ENGINE_OUTAGE: Duration = Duration::from_secs(120);
pub const CDC_OUTAGE: Duration = Duration::from_secs(60);
pub const WRITER_OUTAGE: Duration = Duration::from_secs(30);
pub const CATCHUP_TIMEOUT: Duration = Duration::from_secs(30 * 60);
pub const CONTROLLER_RESTART_EXIT_CODE: i32 = 75;
pub const FAILURE_STEADY_STATE: Duration = Duration::from_secs(120);
pub const FAILURE_ROWS_PER_SECOND: u64 = 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcEndpoint {
    /// Explicit container/network endpoint supplied by the runtime.  It is not
    /// inferred from Compose service names, because the Compose contract has no
    /// standalone CDC service.
    pub network: String,
    pub endpoint: String,
    pub engine: EngineKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureEvidence {
    pub engine: EngineKind,
    pub command: Vec<String>,
    pub signal: String,
    pub started_at_unix_ms: u128,
    pub source_lsn_before: u64,
    pub source_lsn_after: u64,
    pub last_received_lsn: u64,
    pub last_applied_lsn: u64,
    pub restart_duration: Duration,
    pub catchup_duration: Duration,
    pub replay_count: u64,
    pub missing_operations: u64,
    pub duplicate_operations: u64,
    pub out_of_order_operations: u64,
    pub lsn_range_validated: bool,
}

#[async_trait]
pub trait ComposeControl: Send + Sync {
    /// Executes `program` with `args` directly.  Implementations must not pass
    /// either string to a shell.
    async fn invoke(&self, program: &str, args: &[String]) -> Result<CommandOutcome>;

    async fn stop_engine(&self, engine: EngineKind) -> Result<CommandOutcome> {
        self.invoke("docker", &compose_args("stop", service_name(engine)))
            .await
    }

    async fn start_engine(&self, engine: EngineKind) -> Result<CommandOutcome> {
        self.invoke("docker", &compose_args("start", service_name(engine)))
            .await
    }

    async fn disconnect_cdc(&self, endpoint: &CdcEndpoint) -> Result<CommandOutcome> {
        self.invoke(
            "docker",
            &[
                "network".into(),
                "disconnect".into(),
                endpoint.network.clone(),
                endpoint.endpoint.clone(),
            ],
        )
        .await
    }

    async fn reconnect_cdc(&self, endpoint: &CdcEndpoint) -> Result<CommandOutcome> {
        self.invoke(
            "docker",
            &[
                "network".into(),
                "connect".into(),
                endpoint.network.clone(),
                endpoint.endpoint.clone(),
            ],
        )
        .await
    }
}

#[derive(Debug, Clone)]
pub struct SystemComposeControl {
    compose_file: PathBuf,
    project_name: String,
    data_root: PathBuf,
}

impl SystemComposeControl {
    pub fn new(
        compose_file: impl Into<PathBuf>,
        project_name: impl Into<String>,
        data_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            compose_file: compose_file.into(),
            project_name: project_name.into(),
            data_root: data_root.into(),
        }
    }

    fn scoped_compose_args(&self, action: &str, service: &str) -> Vec<String> {
        vec![
            "compose".into(),
            "--project-name".into(),
            self.project_name.clone(),
            "--file".into(),
            self.compose_file.display().to_string(),
            action.into(),
            service.into(),
        ]
    }
}

#[async_trait]
impl ComposeControl for SystemComposeControl {
    async fn invoke(&self, program: &str, args: &[String]) -> Result<CommandOutcome> {
        let output = Command::new(program)
            .args(args)
            .env("R1_DATA_ROOT", &self.data_root)
            .output()
            .with_context(|| format!("starting {program}"))?;
        Ok(CommandOutcome {
            program: program.into(),
            args: args.to_vec(),
            exit_code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }

    async fn stop_engine(&self, engine: EngineKind) -> Result<CommandOutcome> {
        self.invoke(
            "docker",
            &self.scoped_compose_args("stop", service_name(engine)),
        )
        .await
    }

    async fn start_engine(&self, engine: EngineKind) -> Result<CommandOutcome> {
        self.invoke(
            "docker",
            &self.scoped_compose_args("start", service_name(engine)),
        )
        .await
    }
}

#[async_trait]
pub trait FailureWorkload: Send + Sync {
    /// Holds and verifies the requested source rate continuously for the full
    /// duration. Returning early is a contract violation by the implementation.
    async fn wait_for_steady_state(
        &self,
        target_rows_per_second: u64,
        duration: Duration,
    ) -> Result<()>;
    async fn source_rows_written(&self) -> Result<u64>;
    async fn wait_caught_up(&self, engine: EngineKind, timeout: Duration) -> Result<Duration>;
    async fn validate(&self, engine: EngineKind) -> Result<CorrectnessVerdict>;
    async fn stop_writer(&self) -> Result<()>;
    async fn resume_writer(&self) -> Result<()>;
    async fn source_lsn(&self) -> Result<u64>;
    async fn received_lsn(&self, engine: EngineKind) -> Result<u64>;
    async fn applied_lsn(&self, engine: EngineKind) -> Result<u64>;
    async fn replay_count(&self, engine: EngineKind) -> Result<u64>;
    async fn missing_operations(&self, engine: EngineKind, from: u64, through: u64) -> Result<u64>;
    async fn duplicate_operations(
        &self,
        engine: EngineKind,
        from: u64,
        through: u64,
    ) -> Result<u64>;
    async fn out_of_order_operations(
        &self,
        engine: EngineKind,
        from: u64,
        through: u64,
    ) -> Result<u64>;
    async fn validate_lsn_range(
        &self,
        engine: EngineKind,
        _from: u64,
        _through: u64,
    ) -> Result<CorrectnessVerdict>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineFailureResult {
    pub engine: EngineKind,
    pub source_rows_written_while_down: u64,
    pub catchup_duration: Duration,
    pub correctness: CorrectnessVerdict,
    pub commands: Vec<CommandOutcome>,
    pub evidence: FailureEvidence,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureSequenceResult {
    pub engine_failures: Vec<EngineFailureResult>,
    pub cdc_interruptions: Vec<Vec<CommandOutcome>>,
    pub restart_exit_code: i32,
    pub cdc_evidence: Vec<FailureEvidence>,
    pub writer_evidence: Vec<FailureEvidence>,
}

impl EngineFailureResult {
    pub fn caught_up_within(&self, timeout: Duration) -> bool {
        self.catchup_duration <= timeout
    }
}

pub struct FailureRunner<C, W> {
    compose: Arc<C>,
    workload: Arc<W>,
}

impl<C, W> FailureRunner<C, W>
where
    C: ComposeControl,
    W: FailureWorkload,
{
    pub fn new(compose: Arc<C>, workload: Arc<W>) -> Self {
        Self { compose, workload }
    }

    /// Stops exactly the selected engine while leaving the source writer under
    /// the caller's control.  It validates every recovery before returning.
    pub async fn run_engine_kill(
        &self,
        engine: EngineKind,
        outage: Duration,
    ) -> Result<EngineFailureResult> {
        self.wait_for_failure_steady_state().await?;
        let before = self.workload.source_rows_written().await?;
        let source_lsn_before = self.workload.source_lsn().await?;
        let started_at_unix_ms = unix_ms();
        let stopped = self.compose.stop_engine(engine).await?;
        require_success(&stopped)?;
        tokio::time::sleep(outage).await;
        let after = self.workload.source_rows_written().await?;
        let started = self.compose.start_engine(engine).await?;
        require_success(&started)?;
        let catchup_duration = self
            .workload
            .wait_caught_up(engine, CATCHUP_TIMEOUT)
            .await?;
        if catchup_duration > CATCHUP_TIMEOUT {
            bail!("{engine:?} did not catch up within 30 minutes");
        }
        let source_lsn_after = self.workload.source_lsn().await?;
        let correctness = self
            .workload
            .validate_lsn_range(engine, source_lsn_before, source_lsn_after)
            .await?;
        let missing = self
            .workload
            .missing_operations(engine, source_lsn_before, source_lsn_after)
            .await?;
        let duplicates = self
            .workload
            .duplicate_operations(engine, source_lsn_before, source_lsn_after)
            .await?;
        let out_of_order = self
            .workload
            .out_of_order_operations(engine, source_lsn_before, source_lsn_after)
            .await?;
        if !correctness.passed || missing != 0 || duplicates != 0 || out_of_order != 0 {
            bail!("{engine:?} failed post-recovery correctness validation");
        }
        Ok(EngineFailureResult {
            engine,
            source_rows_written_while_down: after.saturating_sub(before),
            catchup_duration,
            correctness,
            commands: vec![stopped, started],
            evidence: FailureEvidence {
                engine,
                command: vec![
                    "docker".into(),
                    "compose".into(),
                    "stop".into(),
                    service_name(engine).into(),
                ],
                signal: "planned-stop".into(),
                started_at_unix_ms,
                source_lsn_before,
                source_lsn_after,
                last_received_lsn: self.workload.received_lsn(engine).await?,
                last_applied_lsn: self.workload.applied_lsn(engine).await?,
                restart_duration: outage,
                catchup_duration,
                replay_count: self.workload.replay_count(engine).await?,
                missing_operations: missing,
                duplicate_operations: duplicates,
                out_of_order_operations: out_of_order,
                lsn_range_validated: true,
            },
        })
    }

    pub async fn run_cdc_disconnect(&self, endpoint: &CdcEndpoint) -> Result<Vec<CommandOutcome>> {
        Ok(self.run_cdc_disconnect_with_evidence(endpoint).await?.0)
    }

    pub async fn run_cdc_disconnect_with_evidence(
        &self,
        endpoint: &CdcEndpoint,
    ) -> Result<(Vec<CommandOutcome>, FailureEvidence)> {
        self.wait_for_failure_steady_state().await?;
        let before = self.workload.source_lsn().await?;
        let started_at_unix_ms = unix_ms();
        let disconnected = self.compose.disconnect_cdc(endpoint).await?;
        require_success(&disconnected)?;
        tokio::time::sleep(CDC_OUTAGE).await;
        let reconnected = self.compose.reconnect_cdc(endpoint).await?;
        require_success(&reconnected)?;
        let after = self.workload.source_lsn().await?;
        let catchup_duration = self
            .workload
            .wait_caught_up(endpoint.engine, CATCHUP_TIMEOUT)
            .await?;
        let verdict = self
            .workload
            .validate_lsn_range(endpoint.engine, before, after)
            .await?;
        let missing = self
            .workload
            .missing_operations(endpoint.engine, before, after)
            .await?;
        let duplicates = self
            .workload
            .duplicate_operations(endpoint.engine, before, after)
            .await?;
        let out_of_order = self
            .workload
            .out_of_order_operations(endpoint.engine, before, after)
            .await?;
        if catchup_duration > CATCHUP_TIMEOUT
            || !verdict.passed
            || missing != 0
            || duplicates != 0
            || out_of_order != 0
        {
            bail!("{:?} failed CDC recovery validation", endpoint.engine);
        }
        let evidence = FailureEvidence {
            engine: endpoint.engine,
            command: vec![
                "docker".into(),
                "network".into(),
                "disconnect".into(),
                endpoint.network.clone(),
                endpoint.endpoint.clone(),
            ],
            signal: "planned-cdc-disconnect".into(),
            started_at_unix_ms,
            source_lsn_before: before,
            source_lsn_after: after,
            last_received_lsn: self.workload.received_lsn(endpoint.engine).await?,
            last_applied_lsn: self.workload.applied_lsn(endpoint.engine).await?,
            restart_duration: CDC_OUTAGE,
            catchup_duration,
            replay_count: self.workload.replay_count(endpoint.engine).await?,
            missing_operations: missing,
            duplicate_operations: duplicates,
            out_of_order_operations: out_of_order,
            lsn_range_validated: true,
        };
        Ok((vec![disconnected, reconnected], evidence))
    }

    pub async fn run_writer_restart(&self) -> Result<()> {
        self.run_writer_restart_with_evidence().await.map(|_| ())
    }

    pub async fn run_writer_restart_with_evidence(&self) -> Result<Vec<FailureEvidence>> {
        self.wait_for_failure_steady_state().await?;
        let before = self.workload.source_lsn().await?;
        let started_at_unix_ms = unix_ms();
        self.workload.stop_writer().await?;
        tokio::time::sleep(WRITER_OUTAGE).await;
        self.workload.resume_writer().await?;
        let after = self.workload.source_lsn().await?;
        let mut evidence = Vec::with_capacity(2);
        for engine in [EngineKind::Graydb, EngineKind::Clickhouse] {
            let catchup_duration = self
                .workload
                .wait_caught_up(engine, CATCHUP_TIMEOUT)
                .await?;
            let verdict = self
                .workload
                .validate_lsn_range(engine, before, after)
                .await?;
            let missing = self
                .workload
                .missing_operations(engine, before, after)
                .await?;
            let duplicates = self
                .workload
                .duplicate_operations(engine, before, after)
                .await?;
            let out_of_order = self
                .workload
                .out_of_order_operations(engine, before, after)
                .await?;
            if catchup_duration > CATCHUP_TIMEOUT
                || !verdict.passed
                || missing != 0
                || duplicates != 0
                || out_of_order != 0
            {
                bail!("{engine:?} failed writer-restart range validation");
            }
            evidence.push(FailureEvidence {
                engine,
                command: vec!["writer".into(), "stop".into(), "resume".into()],
                signal: "planned-writer-restart".into(),
                started_at_unix_ms,
                source_lsn_before: before,
                source_lsn_after: after,
                last_received_lsn: self.workload.received_lsn(engine).await?,
                last_applied_lsn: self.workload.applied_lsn(engine).await?,
                restart_duration: WRITER_OUTAGE,
                catchup_duration,
                replay_count: self.workload.replay_count(engine).await?,
                missing_operations: missing,
                duplicate_operations: duplicates,
                out_of_order_operations: out_of_order,
                lsn_range_validated: true,
            });
        }
        Ok(evidence)
    }

    /// Executes the fixed correctness-mode sequence in spec section 14.  The
    /// caller durably records this outcome before honoring `restart_exit_code`.
    pub async fn run_failure_sequence(
        &self,
        cdc_endpoints: [CdcEndpoint; 2],
    ) -> Result<FailureSequenceResult> {
        let graydb = self
            .run_engine_kill(EngineKind::Graydb, ENGINE_OUTAGE)
            .await?;
        let clickhouse = self
            .run_engine_kill(EngineKind::Clickhouse, ENGINE_OUTAGE)
            .await?;
        let (graydb_cdc, graydb_evidence) = self
            .run_cdc_disconnect_with_evidence(&cdc_endpoints[0])
            .await?;
        let (clickhouse_cdc, clickhouse_evidence) = self
            .run_cdc_disconnect_with_evidence(&cdc_endpoints[1])
            .await?;
        let writer_evidence = self.run_writer_restart_with_evidence().await?;
        Ok(FailureSequenceResult {
            engine_failures: vec![graydb, clickhouse],
            cdc_interruptions: vec![graydb_cdc, clickhouse_cdc],
            restart_exit_code: CONTROLLER_RESTART_EXIT_CODE,
            cdc_evidence: vec![graydb_evidence, clickhouse_evidence],
            writer_evidence,
        })
    }

    async fn wait_for_failure_steady_state(&self) -> Result<()> {
        self.workload
            .wait_for_steady_state(FAILURE_ROWS_PER_SECOND, FAILURE_STEADY_STATE)
            .await
            .context("establishing two-minute 1,000 rows/s failure steady state")
    }
}

fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

pub const fn controller_restart_exit_code() -> i32 {
    CONTROLLER_RESTART_EXIT_CODE
}

fn require_success(outcome: &CommandOutcome) -> Result<()> {
    if outcome.is_success() {
        Ok(())
    } else {
        bail!(
            "{} {:?} failed with {:?}: {}",
            outcome.program,
            outcome.args,
            outcome.exit_code,
            outcome.stderr
        )
    }
}

fn compose_args(action: &str, service: &str) -> Vec<String> {
    vec!["compose".into(), action.into(), service.into()]
}

fn service_name(engine: EngineKind) -> &'static str {
    match engine {
        EngineKind::Graydb => "graydb",
        EngineKind::Clickhouse => "clickhouse",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingCompose;

    #[async_trait]
    impl ComposeControl for RecordingCompose {
        async fn invoke(&self, program: &str, args: &[String]) -> Result<CommandOutcome> {
            Ok(CommandOutcome::succeeded(program, args.to_vec()))
        }
    }

    struct WritesContinue {
        rows: AtomicU64,
        lsn: AtomicU64,
        validations: Mutex<Vec<EngineKind>>,
    }

    #[async_trait]
    impl FailureWorkload for WritesContinue {
        async fn wait_for_steady_state(&self, target: u64, duration: Duration) -> Result<()> {
            assert_eq!(target, FAILURE_ROWS_PER_SECOND);
            assert_eq!(duration, FAILURE_STEADY_STATE);
            tokio::time::sleep(duration).await;
            Ok(())
        }
        async fn source_rows_written(&self) -> Result<u64> {
            Ok(self.rows.fetch_add(1, Ordering::SeqCst))
        }
        async fn wait_caught_up(&self, _: EngineKind, _: Duration) -> Result<Duration> {
            Ok(Duration::from_secs(1))
        }
        async fn validate(&self, _: EngineKind) -> Result<CorrectnessVerdict> {
            Ok(CorrectnessVerdict {
                passed: true,
                differences: Vec::new(),
                invalidations: Vec::new(),
            })
        }
        async fn stop_writer(&self) -> Result<()> {
            Ok(())
        }
        async fn resume_writer(&self) -> Result<()> {
            Ok(())
        }
        async fn source_lsn(&self) -> Result<u64> {
            Ok(self.lsn.fetch_add(1_000, Ordering::SeqCst))
        }
        async fn received_lsn(&self, _: EngineKind) -> Result<u64> {
            Ok(100_000)
        }
        async fn applied_lsn(&self, _: EngineKind) -> Result<u64> {
            Ok(100_000)
        }
        async fn replay_count(&self, _: EngineKind) -> Result<u64> {
            Ok(1)
        }
        async fn missing_operations(&self, _: EngineKind, _: u64, _: u64) -> Result<u64> {
            Ok(0)
        }
        async fn duplicate_operations(&self, _: EngineKind, _: u64, _: u64) -> Result<u64> {
            Ok(0)
        }
        async fn out_of_order_operations(&self, _: EngineKind, _: u64, _: u64) -> Result<u64> {
            Ok(0)
        }
        async fn validate_lsn_range(
            &self,
            engine: EngineKind,
            _: u64,
            _: u64,
        ) -> Result<CorrectnessVerdict> {
            self.validations.lock().unwrap().push(engine);
            self.validate(engine).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn planned_engine_kill_keeps_writes_running_and_validates_catchup() {
        let runner = FailureRunner::new(
            Arc::new(RecordingCompose),
            Arc::new(WritesContinue {
                rows: AtomicU64::new(10),
                lsn: AtomicU64::new(1_000),
                validations: Mutex::new(Vec::new()),
            }),
        );
        let result = runner
            .run_engine_kill(EngineKind::Graydb, ENGINE_OUTAGE)
            .await
            .unwrap();
        assert!(result.source_rows_written_while_down > 0);
        assert!(result.caught_up_within(CATCHUP_TIMEOUT));
        assert!(result.correctness.passed);
    }

    #[tokio::test(start_paused = true)]
    async fn full_sequence_covers_both_engines_cdc_and_writer_restart() {
        let workload = Arc::new(WritesContinue {
            rows: AtomicU64::new(10),
            lsn: AtomicU64::new(1_000),
            validations: Mutex::new(Vec::new()),
        });
        let runner = FailureRunner::new(Arc::new(RecordingCompose), workload.clone());
        let started = tokio::time::Instant::now();
        let result = runner.run_failure_sequence(endpoints()).await.unwrap();
        assert_eq!(result.engine_failures.len(), 2);
        assert_eq!(result.cdc_interruptions.len(), 2);
        assert_eq!(result.restart_exit_code, CONTROLLER_RESTART_EXIT_CODE);
        assert!(result
            .engine_failures
            .iter()
            .all(|failure| failure.evidence.lsn_range_validated));
        assert_eq!(result.cdc_evidence.len(), 2);
        assert_eq!(result.writer_evidence.len(), 2);
        assert!(result
            .writer_evidence
            .iter()
            .all(|evidence| evidence.lsn_range_validated));
        assert_eq!(started.elapsed(), Duration::from_secs(990));
        assert_eq!(
            *workload.validations.lock().unwrap(),
            vec![
                EngineKind::Graydb,
                EngineKind::Clickhouse,
                EngineKind::Graydb,
                EngineKind::Clickhouse,
                EngineKind::Graydb,
                EngineKind::Clickhouse,
            ]
        );
    }

    #[test]
    fn compose_commands_are_argument_vectors() {
        assert_eq!(
            compose_args("stop", "graydb"),
            ["compose", "stop", "graydb"]
        );
    }

    fn endpoints() -> [CdcEndpoint; 2] {
        [
            CdcEndpoint {
                network: "graydb-r1_default".into(),
                endpoint: "graydb".into(),
                engine: EngineKind::Graydb,
            },
            CdcEndpoint {
                network: "graydb-r1_default".into(),
                endpoint: "clickhouse".into(),
                engine: EngineKind::Clickhouse,
            },
        ]
    }
}
