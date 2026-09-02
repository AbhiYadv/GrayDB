use graydb_r1::{
    CommandOutcome, ComposeControl, CorrectnessVerdict, EngineKind, FailureRunner, FailureWorkload,
    RunController, RunStage,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

struct ControllerFixture {
    root: tempfile::TempDir,
    run_id: &'static str,
}

impl ControllerFixture {
    fn new() -> Self {
        Self {
            root: tempdir().expect("temporary run root"),
            run_id: "resume-only-durable-stage",
        }
    }

    fn create(&self) -> RunController {
        RunController::create(self.root.path(), self.run_id).expect("create controller")
    }

    fn resume(&self) -> RunController {
        RunController::resume(self.root.path(), self.run_id).expect("resume controller")
    }
}

#[tokio::test]
async fn resume_starts_after_last_durable_stage_only() {
    let fixture = ControllerFixture::new();
    let mut controller = fixture.create();
    for stage in [
        RunStage::Preflight,
        RunStage::Seed,
        RunStage::BaselineSnapshot,
        RunStage::Bootstrap,
        RunStage::InitialCheckpoint,
        RunStage::Warmup,
        RunStage::Quiet,
    ] {
        controller
            .advance(stage, BTreeMap::new(), || async { Ok(Default::default()) })
            .await
            .expect("complete durable stage");
    }
    controller
        .begin_stage(RunStage::Cdc300, BTreeMap::new())
        .expect("persist incomplete stage before simulated crash");
    drop(controller);

    let resumed = fixture.resume();
    assert_eq!(resumed.next_stage(), Some(RunStage::Cdc300));
    assert_eq!(resumed.execution_count(RunStage::Quiet), 1);
    assert_eq!(resumed.execution_count(RunStage::Cdc300), 0);
}

#[test]
fn invalidated_run_only_allows_report_then_checksums() {
    let fixture = ControllerFixture::new();
    let mut controller = fixture.create();
    controller
        .invalidate(graydb_r1::RunInvalidation::DatasetHashMismatch)
        .expect("persist invalidation");
    assert_eq!(controller.next_stage(), Some(RunStage::Report));
    controller
        .complete_stage(RunStage::Report, Default::default())
        .expect("report stays available after invalidation");
    assert_eq!(controller.next_stage(), Some(RunStage::Checksums));
    controller
        .complete_stage(RunStage::Checksums, Default::default())
        .expect("checksums stay available after invalidation");
    assert_eq!(controller.next_stage(), None);
}

#[test]
fn state_is_written_as_durable_json_not_a_partial_file() {
    let fixture = ControllerFixture::new();
    let mut controller = fixture.create();
    controller
        .complete_stage(RunStage::Preflight, Default::default())
        .expect("persist stage");

    let root = fixture.root.path().join(fixture.run_id);
    assert!(root.join("run-state.json").is_file());
    assert!(!root.join("run-state.json.partial").exists());
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("run-state.json")).unwrap()).unwrap();
    assert_eq!(state["run_id"], fixture.run_id);
}

#[test]
fn restart_request_syncs_state_before_returning_code_75() {
    let fixture = ControllerFixture::new();
    let mut controller = fixture.create();
    assert_eq!(controller.durable_restart_exit_code().unwrap(), 75);
    drop(controller);
    assert_eq!(fixture.resume().next_stage(), Some(RunStage::Preflight));
}

#[derive(Default)]
struct FakeCompose;

#[async_trait::async_trait]
impl ComposeControl for FakeCompose {
    async fn invoke(&self, program: &str, args: &[String]) -> anyhow::Result<CommandOutcome> {
        Ok(CommandOutcome::succeeded(program, args.to_vec()))
    }
}

struct FakeWorkload {
    reads: AtomicU64,
}

#[async_trait::async_trait]
impl FailureWorkload for FakeWorkload {
    async fn source_rows_written(&self) -> anyhow::Result<u64> {
        Ok(self.reads.fetch_add(100, Ordering::SeqCst))
    }

    async fn wait_caught_up(
        &self,
        _engine: EngineKind,
        timeout: Duration,
    ) -> anyhow::Result<Duration> {
        assert_eq!(timeout, Duration::from_secs(1_800));
        Ok(Duration::from_secs(12))
    }

    async fn validate(&self, _engine: EngineKind) -> anyhow::Result<CorrectnessVerdict> {
        Ok(CorrectnessVerdict {
            passed: true,
            differences: Vec::new(),
            invalidations: Vec::new(),
        })
    }

    async fn stop_writer(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn resume_writer(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn planned_engine_kill_keeps_writes_running_and_validates_catchup() {
    let workload = Arc::new(FakeWorkload {
        reads: AtomicU64::new(100),
    });
    let runner = FailureRunner::new(Arc::new(FakeCompose), workload);
    let result = runner
        .run_engine_kill(EngineKind::Graydb, Duration::from_secs(120))
        .await
        .unwrap();
    assert!(result.source_rows_written_while_down > 0);
    assert!(result.caught_up_within(Duration::from_secs(1_800)));
    assert!(result.correctness.passed);
}
