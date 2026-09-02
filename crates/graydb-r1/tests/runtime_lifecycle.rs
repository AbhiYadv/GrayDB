use anyhow::Result;
use async_trait::async_trait;
use graydb_r1::{
    CommandOutcome, DiskSpaceSample, EngineKind, IsolatedReplayEvidence, LifecycleStatus,
    LogicalCheckpoint, MacComposeRuntime, ProfileSpec, QueryId, QueryInvocation, QueryResult,
    R1RuntimeServices, RateSearchObservation, RunController, RunMode, RunPlan, RunStage,
    RuntimeClock, RuntimeStageEvidence, ScaleProfile,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::tempdir;

#[derive(Clone, Default)]
struct FakeClock {
    millis: Arc<AtomicU64>,
}

#[async_trait]
impl RuntimeClock for FakeClock {
    fn elapsed(&self) -> Duration {
        Duration::from_millis(self.millis.load(Ordering::SeqCst))
    }

    fn unix_ms(&self) -> u128 {
        1_800_000_000_000 + u128::from(self.millis.load(Ordering::SeqCst))
    }

    async fn sleep(&self, duration: Duration) {
        self.millis
            .fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct FakeServices {
    calls: Arc<Mutex<Vec<String>>>,
    query_ordinal: u64,
    disk_samples: u64,
    /// When set, every `query_checkpoint` call observes an advanced source
    /// LSN, like independent WAL reads racing an active writer.
    racy_checkpoints: bool,
}

impl FakeServices {
    fn call(&self, value: impl Into<String>) {
        self.calls.lock().unwrap().push(value.into());
    }

    fn artifact(root: &Path, relative: &str) -> Result<RuntimeStageEvidence> {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, b"fixture evidence\n")?;
        Ok(RuntimeStageEvidence {
            command_outcomes: vec![CommandOutcome::succeeded(
                "fixture-service",
                vec![relative.to_owned()],
            )],
            artifact_paths: vec![relative.to_owned()],
        })
    }
}

#[async_trait]
impl R1RuntimeServices for FakeServices {
    async fn preflight(&mut self, root: &Path, _plan: &RunPlan) -> Result<RuntimeStageEvidence> {
        self.call("preflight");
        Self::artifact(root, "environment.json")
    }

    async fn seed(&mut self, root: &Path, _plan: &RunPlan) -> Result<RuntimeStageEvidence> {
        self.call("seed");
        Self::artifact(root, "dataset-manifest.json")
    }

    async fn capture_baseline(
        &mut self,
        root: &Path,
        _plan: &RunPlan,
    ) -> Result<RuntimeStageEvidence> {
        self.call("baseline");
        std::fs::create_dir_all(root.join("baseline/postgres"))?;
        std::fs::write(root.join("baseline/postgres/PG_VERSION"), b"17\n")?;
        Self::artifact(root, "baseline/postgres/SHA256SUMS")
    }

    async fn prepare_isolated_replay(
        &mut self,
        root: &Path,
        _plan: &RunPlan,
    ) -> Result<IsolatedReplayEvidence> {
        self.call("isolated-replay");
        std::fs::create_dir_all(root.join("isolated/graydb"))?;
        std::fs::create_dir_all(root.join("isolated/clickhouse"))?;
        let entry = graydb_r1::ReplayMapEntry {
            logical_sequence: 1,
            original_source_lsn: 100,
            replay_source_lsn: 200,
            operation_sha256: "workload-hash".into(),
        };
        Ok(IsolatedReplayEvidence {
            workload_hashes: vec![vec!["workload-hash".into()], vec!["workload-hash".into()]],
            replay_maps: vec![vec![entry.clone()], vec![entry]],
            logical_checkpoints: vec![1, 1],
        })
    }

    async fn bootstrap(&mut self, root: &Path, _plan: &RunPlan) -> Result<RuntimeStageEvidence> {
        self.call("bootstrap");
        Self::artifact(root, "configs/runtime.json")
    }

    async fn checkpoint(
        &mut self,
        root: &Path,
        stage: RunStage,
        _plan: &RunPlan,
    ) -> Result<RuntimeStageEvidence> {
        self.call(format!("checkpoint:{stage:?}"));
        Self::artifact(root, &format!("correctness/{stage:?}.json"))
    }

    async fn set_writer_rate(&mut self, target_rows_per_sec: Option<u64>) -> Result<()> {
        self.call(format!("writer:{target_rows_per_sec:?}"));
        Ok(())
    }

    async fn query_checkpoint(
        &mut self,
        mode: RunMode,
        engine: EngineKind,
    ) -> Result<LogicalCheckpoint> {
        if engine == EngineKind::Graydb || self.racy_checkpoints {
            self.query_ordinal += 1;
        }
        let source_lsn = match (mode, engine) {
            (RunMode::Isolated, EngineKind::Clickhouse) => 20_000 + self.query_ordinal,
            _ => 10_000 + self.query_ordinal,
        };
        Ok(LogicalCheckpoint {
            sequence: self.query_ordinal,
            source_lsn,
        })
    }

    async fn query(
        &mut self,
        engine: EngineKind,
        invocation: QueryInvocation,
    ) -> Result<QueryResult> {
        self.call(format!("query:{engine:?}:{:?}", invocation.id));
        Ok(QueryResult {
            columns: vec!["status".into(), "count".into()],
            rows: vec![vec![Some("paid".into()), Some("1".into())]],
            target_lsn: invocation.target_lsn,
            visible_lsn: invocation.target_lsn,
            elapsed_ns: 5_000,
            rows_read: Some(1),
            bytes_read: Some(16),
        })
    }

    async fn rate_observation(&mut self, target: u64) -> Result<RateSearchObservation> {
        Ok(RateSearchObservation {
            target_rows_per_sec: target,
            achieved_rows_per_sec: target,
            freshness_p99_ms: 5,
            backlog_bytes: 0,
            backlog_growing: false,
            correctness_passed: true,
            resource_gate: None,
        })
    }

    async fn disk_space(&mut self, _root: &Path) -> Result<DiskSpaceSample> {
        self.disk_samples += 1;
        Ok(DiskSpaceSample {
            total_bytes: 1_000,
            free_bytes: 800,
        })
    }

    async fn failure_sequence(
        &mut self,
        root: &Path,
        _plan: &RunPlan,
    ) -> Result<RuntimeStageEvidence> {
        self.call("failure-sequence");
        Self::artifact(root, "failure-events/sequence.json")
    }

    async fn report(
        &mut self,
        root: &Path,
        _plan: &RunPlan,
        _invalidations: &[graydb_r1::RunInvalidation],
    ) -> Result<RuntimeStageEvidence> {
        self.call("report");
        let mut evidence = Self::artifact(root, "result.json")?;
        Self::artifact(root, "result.md")?;
        evidence.artifact_paths.push("result.md".into());
        Ok(evidence)
    }

    async fn checksums(&mut self, root: &Path) -> Result<RuntimeStageEvidence> {
        self.call("checksums");
        Self::artifact(root, "SHA256SUMS")
    }
}

#[tokio::test]
async fn correctness_mode_captures_one_shared_checkpoint_not_one_per_engine() {
    let temp = tempdir().unwrap();
    let plan = test_plan(RunMode::Correctness);
    let mut controller = RunController::create(temp.path(), "shared-checkpoint").unwrap();
    controller.set_plan(plan.clone()).unwrap();
    let services = FakeServices {
        racy_checkpoints: true,
        ..Default::default()
    };
    let mut runtime = MacComposeRuntime::new(services, FakeClock::default());
    // Correctness mode must capture one shared numeric checkpoint.  Sampling
    // per engine would race the active writer, break checkpoint equality, and
    // abort the run; the single-capture behavior completes the lifecycle.
    assert_eq!(
        controller.run_to_terminal(&mut runtime).await.unwrap(),
        LifecycleStatus::RestartRequired
    );
    drop(controller);
    let mut resumed = RunController::resume(temp.path(), "shared-checkpoint").unwrap();
    assert_eq!(
        resumed.run_to_terminal(&mut runtime).await.unwrap(),
        LifecycleStatus::Complete
    );
}

fn test_plan(mode: RunMode) -> RunPlan {
    RunPlan {
        profile: ScaleProfile::MacSmoke,
        spec: ProfileSpec {
            minimum_bytes: 1,
            repetitions: 1,
            warmup_secs: 1,
            quiet_secs: 1,
            fixed_rate_secs: 1,
            search_step_secs: 1,
            maximum_rate: 2_000,
        },
        mode,
        engines: vec![EngineKind::Graydb, EngineKind::Clickhouse],
        input_hashes: BTreeMap::from([("dataset".into(), "frozen".into())]),
    }
}

#[tokio::test]
async fn realistic_runtime_runs_queries_persists_evidence_and_resumes_after_exit_75() {
    let temp = tempdir().unwrap();
    let plan = test_plan(RunMode::Correctness);
    let mut controller = RunController::create(temp.path(), "runtime-lifecycle").unwrap();
    controller.set_plan(plan.clone()).unwrap();
    let clock = FakeClock::default();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let services = FakeServices {
        calls: calls.clone(),
        ..Default::default()
    };
    let mut runtime = MacComposeRuntime::new(services, clock);

    assert_eq!(
        controller.run_to_terminal(&mut runtime).await.unwrap(),
        LifecycleStatus::RestartRequired
    );
    assert_eq!(controller.next_stage(), Some(RunStage::FinalCheckpoint));
    drop(controller);

    let mut resumed = RunController::resume(temp.path(), "runtime-lifecycle").unwrap();
    assert_eq!(resumed.plan().unwrap().mode, RunMode::Correctness);
    assert_eq!(
        resumed.run_to_terminal(&mut runtime).await.unwrap(),
        LifecycleStatus::Complete
    );

    let run = temp.path().join("runtime-lifecycle");
    assert!(run.join("metrics/Warmup-queries.jsonl").is_file());
    assert!(run
        .join("metrics/RateSearch-2000-rate-observations.jsonl")
        .is_file());
    assert!(run.join("failure-events/sequence.json").is_file());
    assert!(run.join("result.json").is_file());
    assert!(run.join("SHA256SUMS").is_file());

    let state = resumed.state();
    for stage in [
        RunStage::Warmup,
        RunStage::Quiet,
        RunStage::Cdc300,
        RunStage::Cdc1000,
        RunStage::RateSearch,
    ] {
        let record = state.stages.get(&stage).unwrap();
        for engine in [EngineKind::Graydb, EngineKind::Clickhouse] {
            for query in [
                QueryId::Q1,
                QueryId::Q2,
                QueryId::Q3,
                QueryId::Q4,
                QueryId::Q5,
            ] {
                assert!(
                    record
                        .query_records
                        .iter()
                        .filter(|sample| {
                            sample.engine == Some(engine) && sample.query == query && !sample.failed
                        })
                        .count()
                        >= 30
                );
            }
        }
    }
    let calls = calls.lock().unwrap();
    assert!(calls.iter().any(|call| call == "failure-sequence"));
}

#[tokio::test]
async fn isolated_mode_prepares_two_validated_replays_before_any_query() {
    let temp = tempdir().unwrap();
    let plan = test_plan(RunMode::Isolated);
    let mut controller = RunController::create(temp.path(), "isolated-runtime").unwrap();
    controller.set_plan(plan.clone()).unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = MacComposeRuntime::new(
        FakeServices {
            calls: calls.clone(),
            ..Default::default()
        },
        FakeClock::default(),
    );

    assert_eq!(
        controller.run_to_terminal(&mut runtime).await.unwrap(),
        LifecycleStatus::Complete
    );
    let calls = calls.lock().unwrap();
    let replay = calls
        .iter()
        .position(|call| call == "isolated-replay")
        .unwrap();
    let first_query = calls
        .iter()
        .position(|call| call.starts_with("query:"))
        .unwrap();
    assert!(replay < first_query);
    assert!(!calls.iter().any(|call| call == "failure-sequence"));
}
