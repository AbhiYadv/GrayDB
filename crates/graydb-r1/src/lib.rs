pub mod adapter;
pub mod artifacts;
pub mod clickhouse;
pub mod contracts;
pub mod generator;
pub mod graydb;
pub mod ledger;
pub mod manifest;
pub mod oracle;
pub mod preflight;
pub mod query;
pub mod replication;
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
pub use generator::{CopyBatch, DeterministicGenerator, Row, Table};
pub use graydb::GrayDbAdapter;
pub use ledger::{CommitState, CommittedLedger, IntentLog, LedgerEntry};
pub use manifest::{
    BatchManifest, CopySink, DatasetIdentity, DatasetLoader, DatasetManifest, DatasetProbeMetadata,
    PostgresCopySink, PostgresPublishedSizeProbe, PublishedSizeProbe, TableManifest,
    PUBLISHED_TABLE_BYTES_SQL,
};
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
pub use verdict::{CellVerdict, RunInvalidation, Scorecard, WinnerEvaluation};
pub use workload::{
    CustomerRow, Operation, OrderEventRow, OrderRow, RateInterval, RateLimiter, RowMix,
    TransactionPlan, WorkloadPlanner,
};
