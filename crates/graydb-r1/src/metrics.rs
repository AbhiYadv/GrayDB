//! Benchmark measurement primitives. Durations use monotonic clocks; wall time
//! is retained only for human-readable stage metadata.

use anyhow::{anyhow, Context, Result};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LatencySummary {
    pub samples: u64,
    pub p50_micros: u64,
    pub p95_micros: u64,
    pub p99_micros: u64,
    pub max_micros: u64,
}

#[derive(Debug, Clone)]
pub struct LatencySeries {
    histogram: Histogram<u64>,
    samples: Vec<u64>,
}

impl LatencySeries {
    pub fn new(significant_figures: u8) -> Result<Self> {
        Ok(Self {
            histogram: Histogram::new_with_bounds(1, u64::MAX, significant_figures as u8)?,
            samples: Vec::new(),
        })
    }
    pub fn record_micros(&mut self, micros: u64) -> Result<()> {
        let micros = micros.max(1);
        self.histogram.record(micros)?;
        self.samples.push(micros);
        Ok(())
    }
    pub fn summary(&self) -> LatencySummary {
        LatencySummary {
            samples: self.samples.len() as u64,
            p50_micros: self.percentile(0.50),
            p95_micros: self.percentile(0.95),
            p99_micros: self.percentile(0.99),
            max_micros: self.samples.iter().copied().max().unwrap_or(0),
        }
    }
    fn percentile(&self, quantile: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let mut values = self.samples.clone();
        values.sort_unstable();
        values[((values.len() as f64 * quantile).ceil() as usize)
            .saturating_sub(1)
            .min(values.len() - 1)]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryMetricKey {
    pub repetition: u32,
    pub stage: String,
    pub engine: String,
    pub query: crate::query::QueryId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FreshnessMetricKey {
    pub repetition: u32,
    pub stage: String,
    pub engine: String,
}

#[derive(Debug, Clone, Default)]
pub struct Metrics {
    pub query: HashMap<QueryMetricKey, LatencySeries>,
    pub freshness: HashMap<FreshnessMetricKey, LatencySeries>,
    pub resources: Vec<ResourceSample>,
}

impl Metrics {
    pub fn record_query(&mut self, key: QueryMetricKey, micros: u64) -> Result<()> {
        self.query
            .entry(key)
            .or_insert_with(|| LatencySeries::new(3).expect("valid histogram"))
            .record_micros(micros)
    }
    pub fn record_freshness(&mut self, key: FreshnessMetricKey, millis: u64) -> Result<()> {
        self.freshness
            .entry(key)
            .or_insert_with(|| LatencySeries::new(3).expect("valid histogram"))
            .record_micros(millis.saturating_mul(1_000))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageTiming {
    pub stage: String,
    pub elapsed_ns: u128,
    pub started_at_unix_ms: u128,
    pub ended_at_unix_ms: u128,
}

#[derive(Debug)]
pub struct StageTimer {
    stage: String,
    started: Instant,
    started_at_unix_ms: u128,
}

impl StageTimer {
    pub fn start(stage: impl Into<String>) -> Self {
        Self {
            stage: stage.into(),
            started: Instant::now(),
            started_at_unix_ms: wall_unix_ms(),
        }
    }
    pub fn stop(self) -> StageTiming {
        StageTiming {
            stage: self.stage,
            elapsed_ns: self.started.elapsed().as_nanos(),
            started_at_unix_ms: self.started_at_unix_ms,
            ended_at_unix_ms: wall_unix_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSample {
    pub monotonic_ns: u128,
    pub service: String,
    pub cpu_percent: f64,
    pub memory_bytes: u64,
    pub block_read_bytes: u64,
    pub block_write_bytes: u64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
}

impl ResourceSample {
    pub fn from_docker_stats_json(raw: &str, monotonic_ns: u128) -> Result<Self> {
        let value: Value = serde_json::from_str(raw).context("parsing docker stats JSON")?;
        let obj = value
            .as_object()
            .ok_or_else(|| anyhow!("docker stats row is not an object"))?;
        let service =
            text(obj, &["Name", "name"]).ok_or_else(|| anyhow!("docker stats row has no Name"))?;
        let cpu_percent = text(obj, &["CPUPerc", "cpu_percent"])
            .and_then(|v| v.trim_end_matches('%').trim().parse().ok())
            .unwrap_or(0.0);
        let (memory_bytes, _) = pair_bytes(text(obj, &["MemUsage", "memory"]));
        let (block_read_bytes, block_write_bytes) = pair_bytes(text(obj, &["BlockIO", "block_io"]));
        let (network_rx_bytes, network_tx_bytes) = pair_bytes(text(obj, &["NetIO", "net_io"]));
        Ok(Self {
            monotonic_ns,
            service,
            cpu_percent,
            memory_bytes,
            block_read_bytes,
            block_write_bytes,
            network_rx_bytes,
            network_tx_bytes,
        })
    }
}

#[derive(Debug, Default)]
pub struct ResourceSampler {
    pub samples: Vec<ResourceSample>,
}

impl ResourceSampler {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn ingest_json_line(&mut self, raw: &str, monotonic_ns: u128) -> Result<()> {
        self.samples
            .push(ResourceSample::from_docker_stats_json(raw, monotonic_ns)?);
        Ok(())
    }
    pub fn record(&mut self, sample: ResourceSample) {
        self.samples.push(sample);
    }
}

fn text<'a>(obj: &'a serde_json::Map<String, Value>, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        obj.get(*name)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    })
}
fn pair_bytes(value: Option<String>) -> (u64, u64) {
    let Some(value) = value else { return (0, 0) };
    let mut parts = value.split('/').map(str::trim);
    (
        parse_bytes(parts.next().unwrap_or("0")),
        parse_bytes(parts.next().unwrap_or("0")),
    )
}
fn parse_bytes(value: &str) -> u64 {
    let value = value.trim();
    let split = value.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let number = split.trim().parse::<f64>().unwrap_or(0.0);
    let unit = value[split.len()..].trim().to_ascii_lowercase();
    let multiplier = match unit.as_str() {
        "b" | "" => 1.0,
        "kb" => 1_000.0,
        "mb" => 1_000_000.0,
        "gb" => 1_000_000_000.0,
        "kib" => 1024.0,
        "mib" => 1024.0 * 1024.0,
        "gib" => 1024.0 * 1024.0 * 1024.0,
        _ => 1.0,
    };
    (number * multiplier).round().max(0.0) as u64
}
fn wall_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_summary_uses_recorded_samples_without_fastest_run_selection() {
        let mut series = LatencySeries::new(3).unwrap();
        for micros in [1_000, 2_000, 3_000, 4_000, 100_000] {
            series.record_micros(micros).unwrap();
        }
        let s = series.summary();
        assert_eq!(s.samples, 5);
        assert_eq!(s.p50_micros, 3_000);
        assert_eq!(s.max_micros, 100_000);
    }

    #[test]
    fn docker_stats_parser_reads_common_units_and_ignores_malformed_rows() {
        let row = r#"{"Name":"graydb","CPUPerc":"12.5%","MemUsage":"1.5GiB / 4GiB","BlockIO":"2MB / 3MiB","NetIO":"4kB / 5MB"}"#;
        let sample = ResourceSample::from_docker_stats_json(row, 42).unwrap();
        assert_eq!(sample.service, "graydb");
        assert_eq!(sample.monotonic_ns, 42);
        assert_eq!(sample.memory_bytes, 1_610_612_736);
        assert_eq!(sample.block_read_bytes, 2_000_000);
        assert_eq!(sample.block_write_bytes, 3 * 1024 * 1024);
        assert_eq!(sample.network_rx_bytes, 4_000);
        assert_eq!(sample.network_tx_bytes, 5_000_000);
        assert!(ResourceSample::from_docker_stats_json("not json", 42).is_err());
    }
}
