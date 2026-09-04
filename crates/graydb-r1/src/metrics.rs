//! Benchmark measurement primitives. Durations use monotonic clocks; wall time
//! is retained only for human-readable stage metadata.

use anyhow::{anyhow, Context, Result};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
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
    pub raw_samples: Vec<RawMetricSample>,
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
    pub fn write_raw_samples(&self, run_root: impl AsRef<Path>) -> Result<PathBuf> {
        let directory = run_root.as_ref().join("metrics");
        fs::create_dir_all(&directory)?;
        let path = directory.join("metrics.jsonl");
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        for sample in &self.raw_samples {
            serde_json::to_writer(&mut file, sample)?;
            writeln!(file)?;
        }
        Ok(path)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawMetricSample {
    pub kind: String,
    pub monotonic_ns: u128,
    pub stage: Option<String>,
    pub payload: Value,
}
impl RawMetricSample {
    pub fn resource(sample: ResourceSample) -> Self {
        Self {
            kind: "docker_resource".into(),
            monotonic_ns: sample.monotonic_ns,
            stage: None,
            payload: serde_json::to_value(sample).expect("resource serialization"),
        }
    }
    pub fn boundary(sample: BoundarySample) -> Self {
        Self {
            kind: "stage_boundary".into(),
            monotonic_ns: sample.monotonic_ns,
            stage: Some(sample.stage.clone()),
            payload: serde_json::to_value(sample).expect("boundary serialization"),
        }
    }
}

pub trait DockerStatsSource {
    fn sample(&mut self) -> Result<Vec<String>>;
}
pub struct DockerStatsCollector<S> {
    source: S,
    interval: Duration,
    clock: Instant,
    pub sampler: ResourceSampler,
}
impl<S: DockerStatsSource> DockerStatsCollector<S> {
    pub fn new(source: S, interval: Duration) -> Self {
        Self {
            source,
            interval,
            clock: Instant::now(),
            sampler: ResourceSampler::new(),
        }
    }
    pub fn once_per_second(source: S) -> Self {
        Self::new(source, Duration::from_secs(1))
    }
    pub fn interval(&self) -> Duration {
        self.interval
    }
    pub fn collect_ticks(&mut self, ticks: usize) -> Result<Vec<ResourceSample>> {
        let mut collected = Vec::new();
        for _ in 0..ticks {
            let monotonic_ns = self.clock.elapsed().as_nanos();
            for line in self.source.sample()? {
                let sample = ResourceSample::from_docker_stats_json(&line, monotonic_ns)?;
                self.sampler.record(sample.clone());
                collected.push(sample);
            }
            if !self.interval.is_zero() {
                std::thread::sleep(self.interval);
            }
        }
        Ok(collected)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundarySample {
    pub stage: String,
    pub monotonic_ns: u128,
    pub clickhouse_async_metrics: Value,
    pub graydb_status: Value,
}
pub trait ClickHouseMetricsSource {
    fn async_metrics(&mut self) -> Result<Value>;
}
pub trait GrayDbStatusSource {
    fn status(&mut self) -> Result<Value>;
}
pub struct StageBoundaryCollector<C, G> {
    clickhouse: C,
    graydb: G,
    clock: Instant,
}
impl<C: ClickHouseMetricsSource, G: GrayDbStatusSource> StageBoundaryCollector<C, G> {
    pub fn new(clickhouse: C, graydb: G) -> Self {
        Self {
            clickhouse,
            graydb,
            clock: Instant::now(),
        }
    }
    pub fn capture(
        &mut self,
        stage: impl Into<String>,
        monotonic_ns: u128,
    ) -> Result<BoundarySample> {
        Ok(BoundarySample {
            stage: stage.into(),
            monotonic_ns: monotonic_ns.max(self.clock.elapsed().as_nanos()),
            clickhouse_async_metrics: self.clickhouse.async_metrics()?,
            graydb_status: self.graydb.status()?,
        })
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

    #[test]
    fn collectors_capture_boundaries_and_append_raw_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let mut sampler = DockerStatsCollector::new(FakeDocker, Duration::ZERO);
        let raw = sampler.collect_ticks(2).unwrap();
        assert_eq!(raw.len(), 2);
        let mut boundary = StageBoundaryCollector::new(FakeClickHouse, FakeGrayDb);
        let sample = boundary.capture("warmup", 7).unwrap();
        assert_eq!(sample.stage, "warmup");
        let mut metrics = Metrics::default();
        metrics
            .raw_samples
            .extend(raw.into_iter().map(RawMetricSample::resource));
        metrics.raw_samples.push(RawMetricSample::boundary(sample));
        let path = metrics.write_raw_samples(dir.path()).unwrap();
        assert!(path.ends_with("metrics/metrics.jsonl"));
        assert_eq!(std::fs::read_to_string(path).unwrap().lines().count(), 3);
    }

    struct FakeDocker;
    impl DockerStatsSource for FakeDocker {
        fn sample(&mut self) -> Result<Vec<String>> {
            Ok(vec![r#"{"Name":"graydb","CPUPerc":"1%"}"#.into()])
        }
    }
    struct FakeClickHouse;
    impl ClickHouseMetricsSource for FakeClickHouse {
        fn async_metrics(&mut self) -> Result<Value> {
            Ok(serde_json::json!({"Query": 3}))
        }
    }
    struct FakeGrayDb;
    impl GrayDbStatusSource for FakeGrayDb {
        fn status(&mut self) -> Result<Value> {
            Ok(serde_json::json!({"healthy": true}))
        }
    }
}
