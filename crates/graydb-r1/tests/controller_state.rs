use graydb_r1::{
    BenchmarkRuntime, CommandOutcome, ComposeControl, CorrectnessVerdict, EngineKind,
    FailureRunner, FailureWorkload, IsolatedReplayEvidence, LifecycleStatus, ProfileCatalog,
    RunController, RunMode, RunPlan, RunStage, ScaleProfile, StageContext, StageOutcome,
    StageQueryRecord,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
        .begin_stage(RunStage::Report, BTreeMap::new())
        .unwrap();
    controller
        .complete_stage(RunStage::Report, Default::default())
        .expect("report stays available after invalidation");
    assert_eq!(controller.next_stage(), Some(RunStage::Checksums));
    controller
        .begin_stage(RunStage::Checksums, BTreeMap::new())
        .unwrap();
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
        .begin_stage(RunStage::Preflight, BTreeMap::new())
        .unwrap();
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
fn stage_cannot_complete_without_a_durable_start_record() {
    let fixture = ControllerFixture::new();
    let mut controller = fixture.create();
    assert!(controller
        .complete_stage(RunStage::Preflight, Default::default())
        .is_err());
}

#[test]
fn persisted_plan_survives_resume_without_profile_or_mode_substitution() {
    let fixture = ControllerFixture::new();
    let mut controller = fixture.create();
    let catalog = ProfileCatalog::load(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/r1/profiles.toml"),
    )
    .unwrap();
    controller
        .set_plan(RunPlan {
            profile: ScaleProfile::AwsPhase1,
            spec: catalog.get(ScaleProfile::AwsPhase1).unwrap().clone(),
            mode: RunMode::Isolated,
            engines: vec![EngineKind::Clickhouse],
            input_hashes: BTreeMap::new(),
        })
        .unwrap();
    drop(controller);
    let resumed = fixture.resume();
    assert_eq!(resumed.plan().unwrap().profile, ScaleProfile::AwsPhase1);
    assert_eq!(resumed.plan().unwrap().mode, RunMode::Isolated);
    assert_eq!(
        resumed.plan().unwrap().engines,
        vec![EngineKind::Clickhouse]
    );
}

#[derive(Default)]
struct LifecycleRuntime {
    stages: Vec<RunStage>,
}

#[async_trait::async_trait]
impl BenchmarkRuntime for LifecycleRuntime {
    async fn execute_stage(&mut self, context: StageContext<'_>) -> anyhow::Result<StageOutcome> {
        self.stages.push(context.stage);
        let mut outcome = StageOutcome {
            artifact_paths: vec![format!("artifacts/{:?}", context.stage)],
            ..Default::default()
        };
        if context.policy.is_some() {
            for query in [
                graydb_r1::QueryId::Q1,
                graydb_r1::QueryId::Q2,
                graydb_r1::QueryId::Q3,
                graydb_r1::QueryId::Q4,
                graydb_r1::QueryId::Q5,
            ] {
                for _ in 0..30 {
                    outcome.query_records.push(StageQueryRecord {
                        query,
                        engine: Some(EngineKind::Graydb),
                        target_rows_per_sec: None,
                        logical_checkpoint: 1,
                        started_at_unix_ms: 1,
                        completed_at_unix_ms: Some(2),
                        target_lsn: 10,
                        visible_lsn: 10,
                        canonical_digest: "digest".into(),
                        elapsed_ns: 1,
                        freshness_ms: Some(3),
                        rows_read: Some(1),
                        bytes_read: Some(1),
                        failed: false,
                        failure: None,
                    });
                    outcome.query_records.push(StageQueryRecord {
                        query,
                        engine: Some(EngineKind::Clickhouse),
                        target_rows_per_sec: None,
                        logical_checkpoint: 1,
                        started_at_unix_ms: 1,
                        completed_at_unix_ms: Some(2),
                        target_lsn: 10,
                        visible_lsn: 10,
                        canonical_digest: "digest".into(),
                        elapsed_ns: 1,
                        freshness_ms: Some(3),
                        rows_read: Some(1),
                        bytes_read: Some(1),
                        failed: false,
                        failure: None,
                    });
                }
            }
        }
        Ok(outcome)
    }
}

struct BadIsolatedRuntime(LifecycleRuntime);

#[async_trait::async_trait]
impl BenchmarkRuntime for BadIsolatedRuntime {
    async fn execute_stage(&mut self, context: StageContext<'_>) -> anyhow::Result<StageOutcome> {
        self.0.execute_stage(context).await
    }
    async fn prepare_isolated_replay(
        &mut self,
        _context: StageContext<'_>,
    ) -> anyhow::Result<IsolatedReplayEvidence> {
        Ok(IsolatedReplayEvidence {
            workload_hashes: vec![vec!["a".into()], vec!["b".into()]],
            replay_maps: vec![Vec::new(), Vec::new()],
            logical_checkpoints: vec![1, 1],
        })
    }
}

#[tokio::test]
async fn fake_runtime_drives_exact_lifecycle_and_persists_artifacts() {
    let fixture = ControllerFixture::new();
    let mut controller = fixture.create();
    let catalog = ProfileCatalog::load(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/r1/profiles.toml"),
    )
    .unwrap();
    let plan = RunPlan {
        profile: ScaleProfile::MacSmoke,
        spec: catalog.get(ScaleProfile::MacSmoke).unwrap().clone(),
        mode: RunMode::Correctness,
        engines: vec![EngineKind::Graydb, EngineKind::Clickhouse],
        input_hashes: BTreeMap::from([("dataset".into(), "hash".into())]),
    };
    let mut runtime = LifecycleRuntime::default();
    controller.set_plan(plan.clone()).unwrap();
    assert_eq!(
        controller.run_to_terminal(&mut runtime).await.unwrap(),
        LifecycleStatus::Complete
    );
    assert_eq!(runtime.stages, RunStage::ordered());
    assert!(controller
        .state()
        .stages
        .values()
        .all(|record| !record.artifact_paths.is_empty()));
}

#[tokio::test]
async fn isolated_mode_rejects_mismatched_replay_hashes_before_query_stages() {
    let fixture = ControllerFixture::new();
    let mut controller = fixture.create();
    let catalog = ProfileCatalog::load(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/r1/profiles.toml"),
    )
    .unwrap();
    let plan = RunPlan {
        profile: ScaleProfile::MacSmoke,
        spec: catalog.get(ScaleProfile::MacSmoke).unwrap().clone(),
        mode: RunMode::Isolated,
        engines: vec![EngineKind::Graydb, EngineKind::Clickhouse],
        input_hashes: BTreeMap::new(),
    };
    let mut runtime = BadIsolatedRuntime(LifecycleRuntime::default());
    controller.set_plan(plan.clone()).unwrap();
    assert!(controller.run_to_terminal(&mut runtime).await.is_err());
    assert_eq!(
        runtime.0.stages,
        vec![
            RunStage::Preflight,
            RunStage::Seed,
            RunStage::BaselineSnapshot
        ]
    );
    assert_eq!(controller.next_stage(), Some(RunStage::Report));
}

#[tokio::test]
async fn legacy_planless_run_is_invalidated_and_archived_without_benchmark_stages() {
    let fixture = ControllerFixture::new();
    let mut controller = fixture.create();
    // No set_plan call: reproduces run state created before plan persistence.
    let mut runtime = LifecycleRuntime::default();
    let status = controller.run_to_terminal(&mut runtime).await.unwrap();
    assert_eq!(status, LifecycleStatus::InvalidArchived);
    // Report and checksums are archived in-crate; no benchmark stage ran.
    assert!(runtime.stages.is_empty());
    assert_eq!(controller.execution_count(RunStage::Preflight), 0);
    assert_eq!(controller.execution_count(RunStage::Seed), 0);
    match controller.state().invalidations.first() {
        Some(graydb_r1::RunInvalidation::MissingArtifact(reason)) => {
            assert!(reason.contains("run plan"));
        }
        other => panic!("expected exact missing-plan invalidation, got {other:?}"),
    }
    let root = fixture.root.path().join(fixture.run_id);
    assert!(root.join("result.json").is_file());
    assert!(root.join("result.md").is_file());
    assert!(root.join("SHA256SUMS").is_file());
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
    lsn: AtomicU64,
    validations: Mutex<Vec<EngineKind>>,
}

#[async_trait::async_trait]
impl FailureWorkload for FakeWorkload {
    async fn wait_for_steady_state(
        &self,
        target_rows_per_second: u64,
        duration: Duration,
    ) -> anyhow::Result<()> {
        assert_eq!(target_rows_per_second, 1_000);
        assert_eq!(duration, Duration::from_secs(120));
        tokio::time::sleep(duration).await;
        Ok(())
    }

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

    async fn source_lsn(&self) -> anyhow::Result<u64> {
        Ok(self.lsn.fetch_add(1_000, Ordering::SeqCst))
    }

    async fn received_lsn(&self, _engine: EngineKind) -> anyhow::Result<u64> {
        Ok(100_000)
    }

    async fn applied_lsn(&self, _engine: EngineKind) -> anyhow::Result<u64> {
        Ok(100_000)
    }

    async fn replay_count(&self, _engine: EngineKind) -> anyhow::Result<u64> {
        Ok(1)
    }

    async fn missing_operations(
        &self,
        _engine: EngineKind,
        _from: u64,
        _through: u64,
    ) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn duplicate_operations(
        &self,
        _engine: EngineKind,
        _from: u64,
        _through: u64,
    ) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn out_of_order_operations(
        &self,
        _engine: EngineKind,
        _from: u64,
        _through: u64,
    ) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn validate_lsn_range(
        &self,
        engine: EngineKind,
        _from: u64,
        _through: u64,
    ) -> anyhow::Result<CorrectnessVerdict> {
        self.validations.lock().unwrap().push(engine);
        self.validate(engine).await
    }
}

#[tokio::test(start_paused = true)]
async fn planned_engine_kill_keeps_writes_running_and_validates_catchup() {
    let workload = Arc::new(FakeWorkload {
        reads: AtomicU64::new(100),
        lsn: AtomicU64::new(1_000),
        validations: Mutex::new(Vec::new()),
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

#[tokio::test(start_paused = true)]
async fn writer_restart_waits_for_steady_state_and_validates_both_engines() {
    let workload = Arc::new(FakeWorkload {
        reads: AtomicU64::new(100),
        lsn: AtomicU64::new(1_000),
        validations: Mutex::new(Vec::new()),
    });
    let runner = FailureRunner::new(Arc::new(FakeCompose), workload.clone());
    let started = tokio::time::Instant::now();

    runner.run_writer_restart_with_evidence().await.unwrap();

    assert_eq!(started.elapsed(), Duration::from_secs(150));
    assert_eq!(
        *workload.validations.lock().unwrap(),
        vec![EngineKind::Graydb, EngineKind::Clickhouse]
    );
}

#[tokio::test(start_paused = true)]
async fn complete_failure_sequence_has_two_minutes_of_steady_state_before_each_fault() {
    let workload = Arc::new(FakeWorkload {
        reads: AtomicU64::new(100),
        lsn: AtomicU64::new(1_000),
        validations: Mutex::new(Vec::new()),
    });
    let runner = FailureRunner::new(Arc::new(FakeCompose), workload);
    let started = tokio::time::Instant::now();

    runner
        .run_failure_sequence([
            graydb_r1::CdcEndpoint {
                network: "graydb-r1_default".into(),
                endpoint: "graydb".into(),
                engine: EngineKind::Graydb,
            },
            graydb_r1::CdcEndpoint {
                network: "graydb-r1_default".into(),
                endpoint: "clickhouse".into(),
                engine: EngineKind::Clickhouse,
            },
        ])
        .await
        .unwrap();

    assert_eq!(started.elapsed(), Duration::from_secs(990));
}
