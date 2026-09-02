pub mod adapter;
pub mod artifacts;
pub mod clickhouse;
pub mod contracts;
pub mod controller;
pub mod failure;
pub mod generator;
pub mod graydb;
pub mod ledger;
pub mod manifest;
pub mod metrics;
pub mod oracle;
pub mod preflight;
pub mod query;
pub mod replication;
pub mod report;
pub mod verdict;
pub mod workload;

pub use adapter::{EngineAdapter, EngineStatus, QueryInvocation, QueryResult};
pub use artifacts::{sha256_tree, Event, EventLevel, EventRender, EventSink, RunDirectory};
pub use clickhouse::{
    ApplyOutcome, ClickHouseAdapter, ClickHouseCdcAdapter, ClickHouseSink, ClickHouseStatus,
    ReplicationAcknowledger, Version,
};
pub use contracts::{
    EngineKind, LogicalCheckpoint, ProfileCatalog, ProfileSpec, RunConfig, RunMode, ScaleProfile,
};
pub use controller::{
    build_isolated_replays, free_space_sample_interval, pause_for_free_space, rate_search_stop,
    validate_workload_hashes, BaselineSnapshot, BenchmarkRuntime, CommandOutcome,
    IsolatedReplayEvidence, LifecycleStatus, ProcessRunner, QueryStagePolicy,
    RateSearchObservation, RunController, RunPlan, RunStage, RunState, StageContext, StageOutcome,
    StageQueryRecord, StageRecord, SystemProcessRunner,
};
pub use failure::{
    controller_restart_exit_code, CdcEndpoint, ComposeControl, EngineFailureResult,
    FailureEvidence, FailureRunner, FailureSequenceResult, FailureWorkload, SystemComposeControl,
    CATCHUP_TIMEOUT, CDC_OUTAGE, CONTROLLER_RESTART_EXIT_CODE, ENGINE_OUTAGE, WRITER_OUTAGE,
};
pub use generator::{CopyBatch, DeterministicGenerator, Row, Table};
pub use graydb::GrayDbAdapter;
pub use ledger::{CommitState, CommittedLedger, IntentLog, LedgerEntry};
pub use manifest::{
    BatchManifest, CopySink, DatasetIdentity, DatasetLoader, DatasetManifest, DatasetProbeMetadata,
    PostgresCopySink, PostgresPublishedSizeProbe, PublishedSizeProbe, TableManifest,
    PUBLISHED_TABLE_BYTES_SQL,
};
pub use metrics::{
    FreshnessMetricKey, LatencySeries, LatencySummary, Metrics, QueryMetricKey, RawMetricSample,
    ResourceSample, ResourceSampler, StageTimer, StageTiming,
};
pub use oracle::mutation_fixtures;
pub use oracle::{
    CapturedCheckpoint, CheckpointVerdictSink, CorrectnessVerdict, EngineCheckpointEvidence,
    LedgerOracle, PostgresCheckpoint, RowDifference, RowSample, SampledCheckpointEngine,
    VerifiedCheckpoint, WriterControl,
};
pub use preflight::{
    PreflightFailure, PreflightPolicy, PreflightProbe, PreflightReport, PreflightSnapshot,
    SnapshotPreflightProbe, SystemPreflightProbe,
};
pub use query::{canonical_digest, QueryId, QueryParameters, QuerySchedule};
pub use replication::{
    ApplicationWriter, ControlLsnMapper, ControlReplicationConfig, LedgerCommit, ReplayMap,
    ReplayMapEntry, WorkloadReplayer,
};
pub use report::{AwsCapacityRequest, ReportWriter, RunResult};
pub use verdict::{CellVerdict, RunInvalidation, Scorecard, WinnerEvaluation};
pub use workload::{
    CustomerRow, Operation, OrderEventRow, OrderRow, RateInterval, RateLimiter, RowMix,
    TransactionPlan, WorkloadPlanner,
};
