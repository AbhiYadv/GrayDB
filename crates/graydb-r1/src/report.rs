//! Durable benchmark result and markdown report representations.

use crate::contracts::ScaleProfile;
use crate::metrics::{LatencySummary, RawMetricSample, ResourceSample, StageTiming};
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
    pub raw_metrics: Vec<RawMetricSample>,
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
        if let Some(correctness) = &result.correctness {
            out.push_str(&format!(
                "Correctness passed: {}\nDifferences: {}\n",
                correctness.passed,
                correctness.differences.len()
            ));
        } else {
            out.push_str("Correctness passed: not recorded\nDifferences: 0\n");
        }
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
        out.push_str(&format!("\nFreshness p50: {} ms\nFreshness p95: {} ms\nFreshness p99: {} ms\nFreshness samples: {}\n\nSource rate: {} rows/s achieved of {} target\n\nRecovery source_rows_while_down: {}\nRecovery catch-up: {:?}\n\nCPU: {:.2}%\nMemory: {} bytes\nI/O: read {} bytes, write {} bytes, network rx {} bytes, network tx {} bytes\n\nStorage amplification:\n", result.freshness.p50_ms, result.freshness.p95_ms, result.freshness.p99_ms, result.freshness.samples, result.source_rate.achieved_rows_per_sec, result.source_rate.target_rows_per_sec, result.recovery.source_rows_while_down, result.recovery.catchup_ms, result.resources.cpu_percent, result.resources.memory_bytes, result.resources.block_read_bytes, result.resources.block_write_bytes, result.resources.network_rx_bytes, result.resources.network_tx_bytes));
        for (name, value) in &result.storage_amplification {
            out.push_str(&format!("- Storage amplification: {name} = {value:.3}\n"));
        }
        out.push_str("\nCell verdicts:\n");
        for (query, verdict) in &result.cell_verdicts {
            out.push_str(&format!("- Cell verdict: {query} = {verdict:?}\n"));
        }
        if let Some(scorecard) = &result.scorecard {
            out.push_str(&format!("\nAggregate p95 ratio: {:.4}\nAggregate churn ratio: {:.4}\nAggregate wins: {}\nAggregate losses: {}\nAggregate ties: {}\n", scorecard.geometric_p95_ratio, scorecard.churn_ratio, scorecard.wins(), scorecard.losses(), scorecard.ties()));
        } else {
            out.push_str("\nAggregate p95 ratio: n/a\nAggregate churn ratio: n/a\nAggregate wins: n/a\nAggregate losses: n/a\nAggregate ties: n/a\n");
        }
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
        if !result.raw_metrics.is_empty() {
            let metrics_dir = root.join("metrics");
            fs::create_dir_all(&metrics_dir)?;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(metrics_dir.join("metrics.jsonl"))?;
            for sample in &result.raw_metrics {
                serde_json::to_writer(&mut file, sample)?;
                use std::io::Write;
                writeln!(file)?;
            }
        }
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
    pub measured_source_bytes: u64,
    pub measured_graydb_bytes: u64,
    pub measured_clickhouse_bytes: u64,
    pub measured_wal_bytes: u64,
    pub measured_temporary_bytes: u64,
    pub measured_artifact_bytes: u64,
    pub capacity_source_bytes: u64,
    pub capacity_graydb_bytes: u64,
    pub capacity_clickhouse_bytes: u64,
    pub capacity_wal_bytes: u64,
    pub capacity_temporary_bytes: u64,
    pub capacity_artifact_bytes: u64,
    pub recommended_total_bytes: u64,
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
        let measured_graydb = (source as f64 * amp("graydb")).ceil() as u64;
        let measured_clickhouse = (source as f64 * amp("clickhouse")).ceil() as u64;
        let measured_wal = (source as f64 * amp("wal")).ceil() as u64;
        let measured_temporary = (source as f64 * amp("temporary")).ceil() as u64;
        let measured_artifact = (source as f64 * amp("artifact")).ceil() as u64;
        let capacity_source = scaled(source);
        let capacity_graydb = scaled(measured_graydb);
        let capacity_clickhouse = scaled(measured_clickhouse);
        let capacity_wal = scaled(measured_wal);
        let capacity_temporary = scaled(measured_temporary);
        let capacity_artifact = scaled(measured_artifact);
        Self {
            approved: false,
            safety_factor,
            measured_source_bytes: source,
            measured_graydb_bytes: measured_graydb,
            measured_clickhouse_bytes: measured_clickhouse,
            measured_wal_bytes: measured_wal,
            measured_temporary_bytes: measured_temporary,
            measured_artifact_bytes: measured_artifact,
            capacity_source_bytes: capacity_source,
            capacity_graydb_bytes: capacity_graydb,
            capacity_clickhouse_bytes: capacity_clickhouse,
            capacity_wal_bytes: capacity_wal,
            capacity_temporary_bytes: capacity_temporary,
            capacity_artifact_bytes: capacity_artifact,
            recommended_total_bytes: capacity_source
                .saturating_add(capacity_graydb)
                .saturating_add(capacity_clickhouse)
                .saturating_add(capacity_wal)
                .saturating_add(capacity_temporary)
                .saturating_add(capacity_artifact),
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

    #[test]
    fn capacity_preserves_measured_bytes_and_scaled_recommendations() {
        let mut result = RunResult::default();
        result.dataset.bytes = 1_000;
        result.storage_amplification.insert("graydb".into(), 2.0);
        let request = AwsCapacityRequest::from_mac_result(&result);
        assert_eq!(request.measured_graydb_bytes, 2_000);
        assert_eq!(request.capacity_graydb_bytes, 2_700);
        assert!(!request.approved);
    }

    #[test]
    fn report_renders_all_required_evidence_labels() {
        let mut result = RunResult::default();
        result.valid = true;
        result.freshness = FreshnessEvidence {
            p50_ms: 1,
            p95_ms: 2,
            p99_ms: 3,
            samples: 4,
        };
        result.recovery.source_rows_while_down = 8;
        result.storage_amplification.insert("graydb".into(), 1.2);
        result.cell_verdicts.insert("q1".into(), CellVerdict::Tie);
        let report = ReportWriter::render_markdown(&result).unwrap();
        for label in [
            "Freshness p50",
            "Freshness p95",
            "Freshness p99",
            "source_rows_while_down",
            "Storage amplification: graydb",
            "Cell verdict: q1",
            "Correctness passed",
            "Aggregate p95 ratio",
            "Aggregate churn ratio",
        ] {
            assert!(report.contains(label), "missing {label}");
        }
    }
}
