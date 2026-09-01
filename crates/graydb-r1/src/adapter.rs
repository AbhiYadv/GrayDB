use crate::contracts::{EngineKind, LogicalCheckpoint};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryInvocation {
    pub id: crate::query::QueryId,
    pub parameters: crate::query::QueryParameters,
    pub checkpoint: LogicalCheckpoint,
    pub target_lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub target_lsn: u64,
    pub visible_lsn: u64,
    pub elapsed_ns: u128,
    pub rows_read: Option<u64>,
    pub bytes_read: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineStatus {
    pub kind: EngineKind,
    pub healthy: bool,
    pub applied_lsn: Option<u64>,
    pub lag_ms: Option<u64>,
}

#[async_trait]
pub trait EngineAdapter: Send + Sync {
    fn kind(&self) -> EngineKind;

    async fn status(&self) -> Result<EngineStatus>;

    async fn wait_visible(&self, target_lsn: u64, timeout: Duration) -> Result<Duration>;

    async fn query(&self, invocation: &QueryInvocation) -> Result<QueryResult>;
}
