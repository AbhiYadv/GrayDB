//! Planned failure orchestration.  All external process execution crosses the
//! `ComposeControl::invoke` boundary as an executable plus argument vector.

use crate::contracts::EngineKind;
use crate::controller::CommandOutcome;
use crate::oracle::CorrectnessVerdict;
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

pub const ENGINE_OUTAGE: Duration = Duration::from_secs(120);
pub const CDC_OUTAGE: Duration = Duration::from_secs(60);
pub const WRITER_OUTAGE: Duration = Duration::from_secs(30);
pub const CATCHUP_TIMEOUT: Duration = Duration::from_secs(30 * 60);
pub const CONTROLLER_RESTART_EXIT_CODE: i32 = 75;

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

    async fn disconnect_cdc(&self, service: &str) -> Result<CommandOutcome> {
        self.invoke(
            "docker",
            &[
                "network".into(),
                "disconnect".into(),
                "graydb-r1_default".into(),
                service.into(),
            ],
        )
        .await
    }

    async fn reconnect_cdc(&self, service: &str) -> Result<CommandOutcome> {
        self.invoke(
            "docker",
            &[
                "network".into(),
                "connect".into(),
                "graydb-r1_default".into(),
                service.into(),
            ],
        )
        .await
    }
}

#[derive(Debug, Default)]
pub struct SystemComposeControl;

#[async_trait]
impl ComposeControl for SystemComposeControl {
    async fn invoke(&self, program: &str, args: &[String]) -> Result<CommandOutcome> {
        let output = Command::new(program)
            .args(args)
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
}

#[async_trait]
pub trait FailureWorkload: Send + Sync {
    async fn source_rows_written(&self) -> Result<u64>;
    async fn wait_caught_up(&self, engine: EngineKind, timeout: Duration) -> Result<Duration>;
    async fn validate(&self, engine: EngineKind) -> Result<CorrectnessVerdict>;
    async fn stop_writer(&self) -> Result<()>;
    async fn resume_writer(&self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct EngineFailureResult {
    pub engine: EngineKind,
    pub source_rows_written_while_down: u64,
    pub catchup_duration: Duration,
    pub correctness: CorrectnessVerdict,
    pub commands: Vec<CommandOutcome>,
}

#[derive(Debug, Clone)]
pub struct FailureSequenceResult {
    pub engine_failures: Vec<EngineFailureResult>,
    pub cdc_interruptions: Vec<Vec<CommandOutcome>>,
    pub restart_exit_code: i32,
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
        let before = self.workload.source_rows_written().await?;
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
        let correctness = self.workload.validate(engine).await?;
        if !correctness.passed {
            bail!("{engine:?} failed post-recovery correctness validation");
        }
        Ok(EngineFailureResult {
            engine,
            source_rows_written_while_down: after.saturating_sub(before),
            catchup_duration,
            correctness,
            commands: vec![stopped, started],
        })
    }

    pub async fn run_cdc_disconnect(&self, service: &str) -> Result<Vec<CommandOutcome>> {
        let disconnected = self.compose.disconnect_cdc(service).await?;
        require_success(&disconnected)?;
        tokio::time::sleep(CDC_OUTAGE).await;
        let reconnected = self.compose.reconnect_cdc(service).await?;
        require_success(&reconnected)?;
        Ok(vec![disconnected, reconnected])
    }

    pub async fn run_writer_restart(&self) -> Result<()> {
        self.workload.stop_writer().await?;
        tokio::time::sleep(WRITER_OUTAGE).await;
        self.workload.resume_writer().await
    }

    /// Executes the fixed correctness-mode sequence in spec section 14.  The
    /// caller durably records this outcome before honoring `restart_exit_code`.
    pub async fn run_failure_sequence(&self) -> Result<FailureSequenceResult> {
        let graydb = self
            .run_engine_kill(EngineKind::Graydb, ENGINE_OUTAGE)
            .await?;
        let clickhouse = self
            .run_engine_kill(EngineKind::Clickhouse, ENGINE_OUTAGE)
            .await?;
        let graydb_cdc = self.run_cdc_disconnect("graydb").await?;
        let clickhouse_cdc = self.run_cdc_disconnect("clickhouse").await?;
        self.run_writer_restart().await?;
        Ok(FailureSequenceResult {
            engine_failures: vec![graydb, clickhouse],
            cdc_interruptions: vec![graydb_cdc, clickhouse_cdc],
            restart_exit_code: CONTROLLER_RESTART_EXIT_CODE,
        })
    }
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
    }

    #[async_trait]
    impl FailureWorkload for WritesContinue {
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
    }

    #[tokio::test(start_paused = true)]
    async fn planned_engine_kill_keeps_writes_running_and_validates_catchup() {
        let runner = FailureRunner::new(
            Arc::new(RecordingCompose),
            Arc::new(WritesContinue {
                rows: AtomicU64::new(10),
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
        let runner = FailureRunner::new(
            Arc::new(RecordingCompose),
            Arc::new(WritesContinue {
                rows: AtomicU64::new(10),
            }),
        );
        let result = runner.run_failure_sequence().await.unwrap();
        assert_eq!(result.engine_failures.len(), 2);
        assert_eq!(result.cdc_interruptions.len(), 2);
        assert_eq!(result.restart_exit_code, CONTROLLER_RESTART_EXIT_CODE);
    }

    #[test]
    fn compose_commands_are_argument_vectors() {
        assert_eq!(
            compose_args("stop", "graydb"),
            ["compose", "stop", "graydb"]
        );
    }
}
