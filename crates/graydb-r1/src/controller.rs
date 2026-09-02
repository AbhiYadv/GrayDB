//! Durable, resumable R1 stage controller.  This module deliberately owns only
//! orchestration state and safety guards; actual database work is injected at
//! the boundary so the state machine can be exercised without services.

use crate::artifacts::{sha256_tree, Event, EventLevel, EventSink, RunDirectory};
use crate::contracts::{EngineKind, ProfileSpec, RunMode, ScaleProfile};
use crate::verdict::RunInvalidation;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const MINIMUM_QUERY_SAMPLES: u64 = 30;
pub const RUNTIME_FREE_SPACE_FLOOR_PERCENT: u8 = 15;
pub const BACKLOG_LIMIT_BYTES: u64 = 10_u64 << 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStage {
    Preflight,
    Seed,
    BaselineSnapshot,
    Bootstrap,
    InitialCheckpoint,
    Warmup,
    Quiet,
    Cdc300,
    Cdc1000,
    RateSearch,
    FailureSequence,
    FinalCheckpoint,
    Report,
    Checksums,
    Complete,
}

impl RunStage {
    pub const fn ordered() -> &'static [RunStage] {
        &[
            Self::Preflight,
            Self::Seed,
            Self::BaselineSnapshot,
            Self::Bootstrap,
            Self::InitialCheckpoint,
            Self::Warmup,
            Self::Quiet,
            Self::Cdc300,
            Self::Cdc1000,
            Self::RateSearch,
            Self::FailureSequence,
            Self::FinalCheckpoint,
            Self::Report,
            Self::Checksums,
            Self::Complete,
        ]
    }

    pub const fn permits_after_invalidation(self) -> bool {
        matches!(self, Self::Report | Self::Checksums)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandOutcome {
    pub program: String,
    pub args: Vec<String>,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl CommandOutcome {
    pub fn succeeded(program: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            program: program.into(),
            args,
            exit_code: Some(0),
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    pub fn is_success(&self) -> bool {
        self.exit_code == Some(0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageOutcome {
    pub command_outcomes: Vec<CommandOutcome>,
    pub artifact_paths: Vec<String>,
    pub valid: bool,
    pub invalidation: Option<RunInvalidation>,
    #[serde(default)]
    pub query_records: Vec<StageQueryRecord>,
}

impl Default for StageOutcome {
    fn default() -> Self {
        Self {
            command_outcomes: Vec::new(),
            artifact_paths: Vec::new(),
            valid: true,
            invalidation: None,
            query_records: Vec::new(),
        }
    }
}

/// Every timed query observation is retained at the controller boundary.  The
/// runtime must supply the checkpoint/visibility pair instead of treating a
/// query's wall-clock completion as a freshness proof.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageQueryRecord {
    pub query: crate::query::QueryId,
    pub started_at_unix_ms: u128,
    pub target_lsn: u64,
    pub visible_lsn: u64,
    pub canonical_digest: String,
    pub elapsed_ns: u128,
    pub bytes_read: Option<u64>,
    pub failed: bool,
}

#[derive(Debug, Clone)]
pub struct RunPlan {
    pub profile: ScaleProfile,
    pub spec: ProfileSpec,
    pub mode: RunMode,
    pub engines: Vec<EngineKind>,
    pub input_hashes: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct StageContext<'a> {
    pub stage: RunStage,
    pub plan: &'a RunPlan,
    pub policy: Option<QueryStagePolicy>,
    pub run_root: &'a Path,
    pub invalidations: Vec<RunInvalidation>,
}

#[async_trait::async_trait]
pub trait BenchmarkRuntime: Send {
    /// Performs one stage only.  The controller, not the runtime, owns durable
    /// stage transitions, ordering, resume, and invalidation progression.
    async fn execute_stage(&mut self, context: StageContext<'_>) -> Result<StageOutcome>;

    async fn prepare_isolated_replay(
        &mut self,
        _context: StageContext<'_>,
    ) -> Result<IsolatedReplayEvidence> {
        bail!("isolated mode requires a runtime that binds committed ledger replay")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleStatus {
    Complete,
    InvalidArchived,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StageRecord {
    pub input_hashes: BTreeMap<String, String>,
    pub started_at_unix_ms: Option<u128>,
    pub ended_at_unix_ms: Option<u128>,
    pub command_outcomes: Vec<CommandOutcome>,
    pub artifact_paths: Vec<String>,
    pub valid: bool,
    pub completed: bool,
    pub execution_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    pub run_id: String,
    pub stages: BTreeMap<RunStage, StageRecord>,
    pub invalidations: Vec<RunInvalidation>,
}

impl RunState {
    pub fn new(run_id: impl Into<String>) -> Self {
        Self {
            run_id: run_id.into(),
            stages: BTreeMap::new(),
            invalidations: Vec::new(),
        }
    }

    pub fn is_invalid(&self) -> bool {
        !self.invalidations.is_empty()
    }

    pub fn is_complete(&self) -> bool {
        self.stages
            .get(&RunStage::Complete)
            .is_some_and(|record| record.completed)
            && !self.is_invalid()
    }

    pub fn next_stage(&self) -> Option<RunStage> {
        if self.is_invalid() {
            return [RunStage::Report, RunStage::Checksums]
                .into_iter()
                .find(|stage| {
                    !self
                        .stages
                        .get(stage)
                        .is_some_and(|record| record.completed)
                });
        }
        RunStage::ordered().iter().copied().find(|stage| {
            !self
                .stages
                .get(stage)
                .is_some_and(|record| record.completed)
        })
    }
}

pub struct RunController {
    run_directory: RunDirectory,
    events: EventSink,
    state: RunState,
}

impl RunController {
    pub fn create(root: impl AsRef<Path>, run_id: impl AsRef<str>) -> Result<Self> {
        let run_directory = RunDirectory::create(root, run_id.as_ref())?;
        let events = run_directory.event_sink()?;
        let state_path = run_directory.path("run-state.json");
        let state = if state_path.exists() {
            bail!(
                "run state already exists at {}; use resume",
                state_path.display()
            );
        } else {
            RunState::new(run_id.as_ref())
        };
        let mut controller = Self {
            run_directory,
            events,
            state,
        };
        controller.persist()?;
        Ok(controller)
    }

    pub fn resume(root: impl AsRef<Path>, run_id: impl AsRef<str>) -> Result<Self> {
        let run_directory = RunDirectory::create(root, run_id.as_ref())?;
        let events = run_directory.event_sink()?;
        let path = run_directory.path("run-state.json");
        let raw = fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        let state: RunState =
            serde_json::from_slice(&raw).with_context(|| format!("parsing {}", path.display()))?;
        if state.run_id != run_id.as_ref() {
            bail!("run-state run_id does not match requested run id");
        }
        Ok(Self {
            run_directory,
            events,
            state,
        })
    }

    pub fn run_root(&self) -> &Path {
        self.run_directory.root()
    }

    pub fn run_id(&self) -> &str {
        &self.state.run_id
    }

    pub fn state(&self) -> &RunState {
        &self.state
    }

    pub fn next_stage(&self) -> Option<RunStage> {
        self.state.next_stage()
    }

    pub fn execution_count(&self, stage: RunStage) -> u32 {
        self.state
            .stages
            .get(&stage)
            .map(|record| record.execution_count)
            .unwrap_or(0)
    }

    pub fn note(&mut self, message: impl AsRef<str>) -> Result<()> {
        self.events.emit(&Event::info("operator", message.as_ref()))
    }

    /// Flushes the current stage boundary before a controlled controller
    /// restart.  The unfinished stage remains unfinished, so `resume` repeats
    /// it instead of silently skipping any work.
    pub fn durable_restart_exit_code(&mut self) -> Result<i32> {
        self.persist()?;
        self.note("controlled controller restart requested")?;
        Ok(crate::failure::CONTROLLER_RESTART_EXIT_CODE)
    }

    /// Persists the start boundary before a stage is allowed to perform any
    /// external action.  A process crash after this point makes resume replay
    /// this incomplete stage; it never skips it.
    pub fn begin_stage(
        &mut self,
        stage: RunStage,
        input_hashes: BTreeMap<String, String>,
    ) -> Result<()> {
        self.assert_stage_allowed(stage)?;
        let record = self.state.stages.entry(stage).or_default();
        record.input_hashes = input_hashes;
        record.started_at_unix_ms = Some(unix_ms());
        record.ended_at_unix_ms = None;
        record.command_outcomes.clear();
        record.artifact_paths.clear();
        record.valid = true;
        record.completed = false;
        self.persist()?;
        self.emit_stage(stage, "started")
    }

    pub fn complete_stage(&mut self, stage: RunStage, outcome: StageOutcome) -> Result<()> {
        self.assert_stage_allowed(stage)?;
        let record = self
            .state
            .stages
            .get_mut(&stage)
            .ok_or_else(|| anyhow::anyhow!("stage {stage:?} has no durable start record"))?;
        if record.started_at_unix_ms.is_none() || record.completed {
            bail!("stage {stage:?} is not a currently durable started stage");
        }
        record.ended_at_unix_ms = Some(unix_ms());
        record.command_outcomes = outcome.command_outcomes;
        record.artifact_paths = outcome.artifact_paths;
        record.valid = outcome.valid;
        record.completed = true;
        record.execution_count = record.execution_count.saturating_add(1);
        if !outcome.valid {
            self.state
                .invalidations
                .push(outcome.invalidation.unwrap_or_else(|| {
                    RunInvalidation::ResourceSafetyGate(format!("{stage:?} marked invalid"))
                }));
        }
        self.persist()?;
        self.emit_stage(
            stage,
            if outcome.valid {
                "completed"
            } else {
                "invalidated"
            },
        )
    }

    pub async fn advance<F, Fut>(
        &mut self,
        stage: RunStage,
        input_hashes: BTreeMap<String, String>,
        operation: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<StageOutcome>>,
    {
        self.begin_stage(stage, input_hashes)?;
        match operation().await {
            Ok(outcome) => self.complete_stage(stage, outcome),
            Err(error) => {
                self.invalidate(RunInvalidation::UnexpectedProcessExit(format!(
                    "{stage:?}: {error:#}"
                )))?;
                Err(error)
            }
        }
    }

    /// Runs the exact ordered stage list.  Runtime calls are deliberately made
    /// only after `begin_stage` has synced state, and report/checksum stages
    /// remain executable after invalidation while every benchmark stage is
    /// refused by `assert_stage_allowed`.
    pub async fn run_to_terminal<R: BenchmarkRuntime>(
        &mut self,
        plan: &RunPlan,
        runtime: &mut R,
    ) -> Result<LifecycleStatus> {
        while let Some(stage) = self.next_stage() {
            let hashes = plan.input_hashes.clone();
            self.begin_stage(stage, hashes)?;
            let outcome = {
                let context = StageContext {
                    stage,
                    plan,
                    policy: QueryStagePolicy::for_stage(&plan.spec, stage),
                    run_root: self.run_root(),
                    invalidations: self.state.invalidations.clone(),
                };
                let outcome = runtime.execute_stage(context.clone()).await?;
                if stage == RunStage::BaselineSnapshot && plan.mode == RunMode::Isolated {
                    runtime.prepare_isolated_replay(context).await?.validate()?;
                }
                outcome
            };
            if let Some(policy) = QueryStagePolicy::for_stage(&plan.spec, stage) {
                let samples = query_sample_counts(&outcome.query_records);
                if !policy.validates(&samples, policy.scheduled_duration) {
                    self.complete_stage(
                        stage,
                        StageOutcome {
                            valid: false,
                            invalidation: Some(RunInvalidation::MissingArtifact(format!(
                                "{stage:?} did not record at least 30 successful samples for Q1-Q5 within 2x duration"
                            ))),
                            ..outcome
                        },
                    )?;
                    continue;
                }
            }
            self.complete_stage(stage, outcome)?;
        }
        Ok(if self.state.is_invalid() {
            LifecycleStatus::InvalidArchived
        } else {
            LifecycleStatus::Complete
        })
    }

    pub fn invalidate(&mut self, reason: RunInvalidation) -> Result<()> {
        self.state.invalidations.push(reason);
        self.persist()?;
        self.emit_stage(RunStage::Report, "run invalidated")
    }

    fn assert_stage_allowed(&self, stage: RunStage) -> Result<()> {
        if self.state.is_invalid() && !stage.permits_after_invalidation() {
            bail!("invalid run may only advance report and checksums");
        }
        let Some(expected) = self.next_stage() else {
            bail!("run has no stage remaining");
        };
        if stage != expected {
            bail!("stage {stage:?} is out of order; expected {expected:?}");
        }
        Ok(())
    }

    fn persist(&mut self) -> Result<()> {
        persist_state(self.run_directory.root(), &self.state)
    }

    fn emit_stage(&mut self, stage: RunStage, message: &str) -> Result<()> {
        self.events.emit(&Event::new(
            EventLevel::Stage,
            format!("{stage:?}"),
            "controller",
            message,
        ))
    }
}

fn query_sample_counts(records: &[StageQueryRecord]) -> Vec<u64> {
    use crate::query::QueryId;
    [
        QueryId::Q1,
        QueryId::Q2,
        QueryId::Q3,
        QueryId::Q4,
        QueryId::Q5,
    ]
    .iter()
    .map(|query| {
        records
            .iter()
            .filter(|record| record.query == *query && !record.failed)
            .count() as u64
    })
    .collect()
}

fn persist_state(root: &Path, state: &RunState) -> Result<()> {
    let partial = root.join("run-state.json.partial");
    let final_path = root.join("run-state.json");
    let bytes = serde_json::to_vec_pretty(state)?;
    {
        let mut file =
            File::create(&partial).with_context(|| format!("creating {}", partial.display()))?;
        use std::io::Write;
        file.write_all(&bytes)
            .with_context(|| format!("writing {}", partial.display()))?;
        file.sync_all()
            .with_context(|| format!("syncing {}", partial.display()))?;
    }
    fs::rename(&partial, &final_path)
        .with_context(|| format!("renaming {}", final_path.display()))?;
    File::open(root)
        .with_context(|| format!("opening state directory {}", root.display()))?
        .sync_all()
        .with_context(|| format!("syncing state directory {}", root.display()))?;
    Ok(())
}

pub trait ProcessRunner: Send + Sync {
    fn run(&self, program: &str, args: &[String]) -> Result<CommandOutcome>;
}

/// The only real-process adapter used by this crate.  It deliberately passes an
/// argument vector directly to `Command`; it never invokes a shell.
#[derive(Debug, Default)]
pub struct SystemProcessRunner;

impl ProcessRunner for SystemProcessRunner {
    fn run(&self, program: &str, args: &[String]) -> Result<CommandOutcome> {
        let output = std::process::Command::new(program)
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineSnapshot {
    pub postgres_dir: PathBuf,
    pub checksum_path: PathBuf,
}

impl BaselineSnapshot {
    pub fn capture(run_root: impl AsRef<Path>, runner: &dyn ProcessRunner) -> Result<Self> {
        let run_root = run_root.as_ref();
        let postgres_dir = run_root.join("baseline/postgres");
        if postgres_dir.exists() {
            bail!("baseline already exists and will not be overwritten");
        }
        fs::create_dir_all(postgres_dir.parent().expect("baseline parent"))?;
        let args = vec!["-D".into(), postgres_dir.display().to_string()];
        let outcome = runner.run("pg_basebackup", &args)?;
        if !outcome.is_success() {
            bail!("pg_basebackup failed: {}", outcome.stderr);
        }
        if !postgres_dir.is_dir() {
            bail!("pg_basebackup did not create {}", postgres_dir.display());
        }
        let checksum_path = sha256_tree(&postgres_dir)?;
        Ok(Self {
            postgres_dir,
            checksum_path,
        })
    }

    /// Restores to a destination that must not have existed before the call.
    /// The baseline is only read, so retries cannot corrupt it.
    pub fn restore_isolated(
        &self,
        run_root: impl AsRef<Path>,
        engine: EngineKind,
    ) -> Result<PathBuf> {
        let engine_name = match engine {
            EngineKind::Graydb => "graydb",
            EngineKind::Clickhouse => "clickhouse",
        };
        let destination = run_root
            .as_ref()
            .join("isolated")
            .join(engine_name)
            .join("postgres");
        if destination.exists() {
            bail!(
                "isolated PostgreSQL directory already exists and will not be overwritten: {}",
                destination.display()
            );
        }
        copy_tree(&self.postgres_dir, &destination)?;
        Ok(destination)
    }
}

fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    if !source.is_dir() {
        bail!("baseline source is not a directory: {}", source.display());
    }
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), &target)
                .with_context(|| format!("copying baseline file to {}", target.display()))?;
        }
    }
    Ok(())
}

pub fn validate_workload_hashes(expected: &[String], replayed: &[Vec<String>]) -> Result<()> {
    if replayed.is_empty() || replayed.iter().any(|hashes| hashes != expected) {
        bail!("isolated replay workload hashes do not match the canonical intent plan");
    }
    Ok(())
}

/// Durable isolated replay evidence.  The coordinator builds these entries from
/// the committed ledger through `WorkloadReplayer`; validation happens before a
/// query stage is allowed to start.
#[derive(Debug, Clone)]
pub struct IsolatedReplayEvidence {
    pub workload_hashes: Vec<Vec<String>>,
    pub replay_maps: Vec<Vec<crate::replication::ReplayMapEntry>>,
    pub logical_checkpoints: Vec<u64>,
}

impl IsolatedReplayEvidence {
    pub fn validate(&self) -> Result<()> {
        let expected = self
            .workload_hashes
            .first()
            .ok_or_else(|| anyhow::anyhow!("isolated replay has no workload hash"))?;
        validate_workload_hashes(expected, &self.workload_hashes)?;
        if self.replay_maps.len() != 2 || self.logical_checkpoints.len() != 2 {
            bail!("isolated mode requires exactly two replay maps and matching checkpoints");
        }
        if self.logical_checkpoints[0] != self.logical_checkpoints[1] {
            bail!("isolated engines did not reach the same logical checkpoint");
        }
        let first = &self.replay_maps[0];
        for map in &self.replay_maps[1..] {
            if first.len() != map.len()
                || first.iter().zip(map).any(|(a, b)| {
                    a.logical_sequence != b.logical_sequence
                        || a.original_source_lsn != b.original_source_lsn
                        || a.operation_sha256 != b.operation_sha256
                })
            {
                bail!("isolated replay maps do not represent the same committed ledger");
            }
        }
        Ok(())
    }
}

/// Restores the immutable baseline separately for both engines and writes each
/// sequence-to-LSN map through Task 8's `WorkloadReplayer`.  The caller supplies
/// only committed plans/ledger entries; uncommitted intent cannot enter replay.
pub fn build_isolated_replays(
    snapshot: &BaselineSnapshot,
    run_root: &Path,
    replays: &[(
        EngineKind,
        Vec<(
            crate::workload::TransactionPlan,
            crate::ledger::LedgerEntry,
            u64,
        )>,
    )],
) -> Result<IsolatedReplayEvidence> {
    if replays.len() != 2 {
        bail!("isolated mode requires exactly GrayDB and ClickHouse replays");
    }
    let mut workload_hashes = Vec::new();
    let mut replay_maps = Vec::new();
    let mut logical_checkpoints = Vec::new();
    for (engine, entries) in replays {
        snapshot.restore_isolated(run_root, *engine)?;
        let engine_name = match engine {
            EngineKind::Graydb => "graydb",
            EngineKind::Clickhouse => "clickhouse",
        };
        let map_dir = run_root.join("isolated").join(engine_name);
        let mut replayer = crate::replication::WorkloadReplayer::new(
            crate::replication::ReplayMap::create(&map_dir)?,
        );
        replayer.replay(entries)?;
        let map = replayer.into_replay_map();
        workload_hashes.push(
            entries
                .iter()
                .map(|(plan, _, _)| plan.operation_sha256.clone())
                .collect(),
        );
        logical_checkpoints.push(
            map.entries()
                .last()
                .map(|entry| entry.logical_sequence)
                .unwrap_or(0),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryStagePolicy {
    pub scheduled_duration: Duration,
    pub maximum_duration: Duration,
    pub minimum_samples_per_query: u64,
}

impl QueryStagePolicy {
    pub fn for_stage(profile: &ProfileSpec, stage: RunStage) -> Option<Self> {
        let seconds = match stage {
            RunStage::Warmup => profile.warmup_secs,
            RunStage::Quiet => profile.quiet_secs,
            RunStage::Cdc300 | RunStage::Cdc1000 => profile.fixed_rate_secs,
            RunStage::RateSearch => profile.search_step_secs,
            _ => return None,
        };
        let scheduled_duration = Duration::from_secs(seconds);
        Some(Self {
            scheduled_duration,
            maximum_duration: scheduled_duration.saturating_mul(2),
            minimum_samples_per_query: MINIMUM_QUERY_SAMPLES,
        })
    }

    pub fn validates(&self, query_samples: &[u64], elapsed: Duration) -> bool {
        elapsed <= self.maximum_duration
            && query_samples.len() == 5
            && query_samples
                .iter()
                .all(|samples| *samples >= self.minimum_samples_per_query)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateSearchObservation {
    pub target_rows_per_sec: u64,
    pub achieved_rows_per_sec: u64,
    pub freshness_p99_ms: u64,
    pub backlog_bytes: u64,
    pub backlog_growing: bool,
    pub correctness_passed: bool,
    pub resource_gate: Option<String>,
}

pub fn rate_search_stop(observations: &[RateSearchObservation]) -> Option<RunInvalidation> {
    let latest = observations.last()?;
    if let Some(reason) = &latest.resource_gate {
        return Some(RunInvalidation::ResourceSafetyGate(reason.clone()));
    }
    if !latest.correctness_passed {
        return Some(RunInvalidation::ResultDigestMismatch {
            query: crate::query::QueryId::Q1,
            checkpoint: 0,
        });
    }
    if latest.freshness_p99_ms > 1_000 {
        return Some(RunInvalidation::FreshnessP99Exceeded {
            limit_ms: 1_000,
            actual_ms: latest.freshness_p99_ms,
        });
    }
    let recent = observations.iter().rev().take(3).collect::<Vec<_>>();
    if recent.len() == 3
        && recent.iter().all(|sample| {
            sample.achieved_rows_per_sec.saturating_mul(100)
                < sample.target_rows_per_sec.saturating_mul(95)
        })
    {
        return Some(RunInvalidation::SourceRateMissed {
            target: latest.target_rows_per_sec,
            achieved: latest.achieved_rows_per_sec,
        });
    }
    if recent.len() == 3
        && recent
            .iter()
            .all(|sample| sample.backlog_bytes > BACKLOG_LIMIT_BYTES && sample.backlog_growing)
    {
        return Some(RunInvalidation::ResourceSafetyGate(
            "CDC backlog exceeded 10 GiB and grew for three intervals".into(),
        ));
    }
    None
}

pub fn pause_for_free_space(total_bytes: u64, free_bytes: u64) -> bool {
    total_bytes == 0
        || free_bytes.saturating_mul(100)
            < total_bytes.saturating_mul(u64::from(RUNTIME_FREE_SPACE_FLOOR_PERCENT))
}

pub const fn free_space_sample_interval() -> Duration {
    Duration::from_secs(1)
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn stop_rules_require_three_consecutive_rate_misses() {
        let samples = (0..3)
            .map(|_| RateSearchObservation {
                target_rows_per_sec: 1_000,
                achieved_rows_per_sec: 949,
                freshness_p99_ms: 1,
                backlog_bytes: 0,
                backlog_growing: false,
                correctness_passed: true,
                resource_gate: None,
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            rate_search_stop(&samples),
            Some(RunInvalidation::SourceRateMissed { .. })
        ));
    }

    #[test]
    fn isolated_restore_never_overwrites_existing_destination() {
        let root = tempdir().unwrap();
        let baseline = root.path().join("baseline/postgres");
        fs::create_dir_all(&baseline).unwrap();
        fs::write(baseline.join("PG_VERSION"), "17").unwrap();
        let snapshot = BaselineSnapshot {
            postgres_dir: baseline,
            checksum_path: root.path().join("baseline/postgres/SHA256SUMS"),
        };
        snapshot
            .restore_isolated(root.path(), EngineKind::Graydb)
            .unwrap();
        assert!(snapshot
            .restore_isolated(root.path(), EngineKind::Graydb)
            .is_err());
    }

    #[test]
    fn stop_rules_cover_freshness_backlog_correctness_and_free_space() {
        let base = RateSearchObservation {
            target_rows_per_sec: 1_000,
            achieved_rows_per_sec: 1_000,
            freshness_p99_ms: 1,
            backlog_bytes: 0,
            backlog_growing: false,
            correctness_passed: true,
            resource_gate: None,
        };
        let mut freshness = base.clone();
        freshness.freshness_p99_ms = 1_001;
        assert!(matches!(
            rate_search_stop(&[freshness]),
            Some(RunInvalidation::FreshnessP99Exceeded { .. })
        ));
        let mut incorrect = base.clone();
        incorrect.correctness_passed = false;
        assert!(matches!(
            rate_search_stop(&[incorrect]),
            Some(RunInvalidation::ResultDigestMismatch { .. })
        ));
        let mut backlog = base;
        backlog.backlog_bytes = BACKLOG_LIMIT_BYTES + 1;
        backlog.backlog_growing = true;
        assert!(matches!(
            rate_search_stop(&[backlog.clone(), backlog.clone(), backlog]),
            Some(RunInvalidation::ResourceSafetyGate(_))
        ));
        assert!(pause_for_free_space(100, 14));
        assert!(!pause_for_free_space(100, 15));
    }

    #[test]
    fn isolated_replay_coordinator_writes_two_maps_and_rejects_hash_mismatch() {
        use crate::ledger::LedgerEntry;
        use crate::workload::WorkloadPlanner;
        let root = tempdir().unwrap();
        let baseline = root.path().join("baseline/postgres");
        fs::create_dir_all(&baseline).unwrap();
        fs::write(baseline.join("PG_VERSION"), "17").unwrap();
        let snapshot = BaselineSnapshot {
            postgres_dir: baseline,
            checksum_path: root.path().join("unused"),
        };
        let plan = WorkloadPlanner::new(20260901).plan(1);
        let entry = LedgerEntry {
            sequence: 1,
            xid: 1,
            source_lsn: 100,
            operation_sha256: plan.operation_sha256.clone(),
            committed_unix_ms: 0,
            previous_entry_sha256: String::new(),
            entry_sha256: String::new(),
        };
        let good = vec![(plan.clone(), entry.clone(), 200)];
        let evidence = build_isolated_replays(
            &snapshot,
            root.path(),
            &[
                (EngineKind::Graydb, good.clone()),
                (EngineKind::Clickhouse, good),
            ],
        )
        .unwrap();
        assert_eq!(evidence.replay_maps.len(), 2);
        assert!(root
            .path()
            .join("isolated/graydb/replay-map.jsonl")
            .is_file());
        let bad_root = tempdir().unwrap();
        let bad_baseline = bad_root.path().join("baseline/postgres");
        fs::create_dir_all(&bad_baseline).unwrap();
        fs::write(bad_baseline.join("PG_VERSION"), "17").unwrap();
        let bad_snapshot = BaselineSnapshot {
            postgres_dir: bad_baseline,
            checksum_path: bad_root.path().join("unused"),
        };
        let bad = LedgerEntry {
            operation_sha256: "bad".into(),
            ..entry
        };
        assert!(build_isolated_replays(
            &bad_snapshot,
            bad_root.path(),
            &[
                (EngineKind::Graydb, vec![(plan.clone(), bad, 200)]),
                (
                    EngineKind::Clickhouse,
                    vec![(
                        plan.clone(),
                        LedgerEntry {
                            sequence: 1,
                            xid: 1,
                            source_lsn: 100,
                            operation_sha256: plan.operation_sha256.clone(),
                            committed_unix_ms: 0,
                            previous_entry_sha256: String::new(),
                            entry_sha256: String::new()
                        },
                        200
                    )]
                )
            ]
        )
        .is_err());
    }
}
