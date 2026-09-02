//! Durable benchmark result and markdown report representations.

use crate::contracts::ScaleProfile;
use crate::metrics::{LatencySummary, ResourceSample, StageTiming};
use crate::oracle::CorrectnessVerdict;
use crate::verdict::{CellVerdict, RunInvalidation, Scorecard, WinnerEvaluation};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatasetEvidence {
    pub bytes: u64,
    pub rows: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FreshnessEvidence {
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub samples: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourceRateEvidence {
    pub target_rows_per_sec: u64,
    pub achieved_rows_per_sec: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecoveryEvidence {
    pub catchup_ms: Option<u64>,
    pub source_rows_while_down: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceEvidence {
    pub cpu_percent: f64,
    pub memory_bytes: u64,
    pub block_read_bytes: u64,
    pub block_write_bytes: u64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RunResult {
    pub benchmark_id: String,
    pub profile: Option<ScaleProfile>,
    pub valid: bool,
    pub total_time_ns: u128,
    pub operation_times: BTreeMap<String, StageTiming>,
    pub dataset: DatasetEvidence,
    pub correctness: Option<CorrectnessVerdict>,
    pub invalidations: Vec<RunInvalidation>,
    pub query_latency: BTreeMap<String, LatencySummary>,
    pub freshness: FreshnessEvidence,
    pub source_rate: SourceRateEvidence,
    pub recovery: RecoveryEvidence,
    pub resources: ResourceEvidence,
    pub storage_amplification: BTreeMap<String, f64>,
    pub cell_verdicts: BTreeMap<String, CellVerdict>,
    pub scorecard: Option<Scorecard>,
    pub winner: Option<WinnerEvaluation>,
    pub resource_samples: Vec<ResourceSample>,
    pub artifact_paths: Vec<String>,
}

impl RunResult {
    pub fn invalid(reason: RunInvalidation) -> Self {
        Self {
            benchmark_id: "R1-P1-v1".into(),
            profile: Some(ScaleProfile::MacSmoke),
            invalidations: vec![reason],
            ..Default::default()
        }
    }
    pub fn metric_samples(&self) -> u64 {
        if !self.valid {
            return 0;
        }
        self.query_latency.values().map(|s| s.samples).sum::<u64>()
            + self.freshness.samples
            + self.resource_samples.len() as u64
    }
}

pub struct ReportWriter;
impl ReportWriter {
    pub fn render_markdown(result: &RunResult) -> Result<String> {
        let status = if result.valid { "VALID" } else { "INVALID" };
        let mut out = format!(
            "# R1 benchmark result\n\n## Validity: {status}\n\nTotal time: {} ns\n\n",
            result.total_time_ns
        );
        out.push_str("## Operation time\n\n| Stage | Elapsed (ns) |\n|---|---:|\n");
        for (stage, timing) in &result.operation_times {
            out.push_str(&format!("| {stage} | {} |\n", timing.elapsed_ns));
        }
        out.push_str(&format!(
            "\nDataset: {} bytes, {} rows\n\n",
            result.dataset.bytes, result.dataset.rows
        ));
        out.push_str("## Correctness\n\n");
        if result.valid {
            out.push_str("All correctness checks passed.\n\n");
        } else {
            out.push_str("Correctness invalidated; performance metrics and winner language are suppressed.\n\n");
        }
        if !result.invalidations.is_empty() {
            out.push_str("Invalidations:\n");
            for reason in &result.invalidations {
                out.push_str(&format!("- {reason:?}\n"));
            }
            out.push('\n');
        }
        out.push_str("## Query latency\n\n| Query | p50 (us) | p95 (us) | p99 (us) | max (us) | samples |\n|---|---:|---:|---:|---:|---:|\n");
        if result.valid {
            for (query, s) in &result.query_latency {
                out.push_str(&format!(
                    "| {query} | {} | {} | {} | {} | {} |\n",
                    s.p50_micros, s.p95_micros, s.p99_micros, s.max_micros, s.samples
                ));
            }
        }
        out.push_str(&format!("\nFreshness p99: {} ms ({} samples)\n\nSource rate: {} rows/s achieved of {} target\n\nRecovery catch-up: {:?}\n\nCPU: {:.2}%\nMemory: {} bytes\nI/O: read {} bytes, write {} bytes\n\nStorage amplification: {:?}\n\nCell verdicts: {:?}\n", result.freshness.p99_ms, result.freshness.samples, result.source_rate.achieved_rows_per_sec, result.source_rate.target_rows_per_sec, result.recovery.catchup_ms, result.resources.cpu_percent, result.resources.memory_bytes, result.resources.block_read_bytes, result.resources.block_write_bytes, result.storage_amplification, result.cell_verdicts));
        if result.valid {
            if let Some(winner) = &result.winner {
                if winner.graydb_beat_clickhouse() {
                    out.push_str("\nConclusion: GrayDB beat ClickHouse.\n");
                } else {
                    out.push_str("\nConclusion: no overall winner.\n");
                }
            } else {
                out.push_str("\nConclusion: no overall winner.\n");
            }
        }
        Ok(out)
    }
    pub fn write_to_dir(root: impl AsRef<Path>, result: &RunResult) -> Result<()> {
        let root = root.as_ref();
        fs::create_dir_all(root).with_context(|| format!("creating {}", root.display()))?;
        fs::write(root.join("result.json"), serde_json::to_vec_pretty(result)?)
            .context("writing result.json")?;
        fs::write(root.join("result.md"), Self::render_markdown(result)?)
            .context("writing result.md")?;
        fs::write(
            root.join("aws-capacity-request.json"),
            serde_json::to_vec_pretty(&AwsCapacityRequest::from_mac_result(result))?,
        )
        .context("writing aws-capacity-request.json")?;
        Ok(())
    }
    pub fn write(root: impl AsRef<Path>, result: &RunResult) -> Result<()> {
        Self::write_to_dir(root, result)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsCapacityRequest {
    pub approved: bool,
    pub safety_factor: f64,
    pub source_bytes: u64,
    pub graydb_bytes: u64,
    pub clickhouse_bytes: u64,
    pub wal_bytes: u64,
    pub temporary_bytes: u64,
    pub artifact_bytes: u64,
    pub total_bytes: u64,
}
impl AwsCapacityRequest {
    pub fn from_mac_result(result: &RunResult) -> Self {
        let amp = |name: &str| {
            result
                .storage_amplification
                .get(name)
                .copied()
                .unwrap_or(1.0)
                .max(0.0)
        };
        let source = result.dataset.bytes;
        let safety_factor = 1.35;
        let scaled = |bytes: u64| ((bytes as f64) * safety_factor).ceil() as u64;
        let graydb = scaled((source as f64 * amp("graydb")).ceil() as u64);
        let clickhouse = scaled((source as f64 * amp("clickhouse")).ceil() as u64);
        let wal = scaled((source as f64 * amp("wal")).ceil() as u64);
        let temporary = scaled((source as f64 * amp("temporary")).ceil() as u64);
        let artifact = scaled((source as f64 * amp("artifact")).ceil() as u64);
        Self {
            approved: false,
            safety_factor,
            source_bytes: scaled(source),
            graydb_bytes: graydb,
            clickhouse_bytes: clickhouse,
            wal_bytes: wal,
            temporary_bytes: temporary,
            artifact_bytes: artifact,
            total_bytes: scaled(source)
                .saturating_add(graydb)
                .saturating_add(clickhouse)
                .saturating_add(wal)
                .saturating_add(temporary)
                .saturating_add(artifact),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verdict::RunInvalidation;

    #[test]
    fn invalid_run_report_never_contains_a_winner() {
        let result = RunResult::invalid(RunInvalidation::MissingSequence(9));
        let report = ReportWriter::render_markdown(&result).unwrap();
        assert!(report.contains("INVALID"));
        assert!(!report.contains("GrayDB beat ClickHouse"));
        assert!(!report.contains("ClickHouse beat GrayDB"));
    }

    #[test]
    fn invalid_result_has_zero_samples_and_one_invalidation() {
        let result = RunResult::invalid(RunInvalidation::MissingSequence(9));
        assert!(!result.valid);
        assert_eq!(result.metric_samples(), 0);
        assert_eq!(result.invalidations.len(), 1);
        assert!(result.winner.is_none());
    }
}
