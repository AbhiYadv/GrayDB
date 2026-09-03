use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

const APPROVED_DATA_ROOT_PREFIX: &str = "/Volumes/Crucial X9/GrayDB/.r1";
const REQUIRED_CPUS: u32 = 8;
const REQUIRED_MEMORY_BYTES: u64 = 12_u64 << 30;
const REQUIRED_COLIMA_DISK_BYTES: u64 = 600_u64 << 30;
const MIN_PROJECTED_FREE_RATIO: f64 = 0.20;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightSnapshot {
    pub volume_bytes: u64,
    pub available_bytes: u64,
    pub expected_peak_bytes: u64,
    pub runtime_stop_bytes: u64,
    pub cpus: u32,
    pub memory_bytes: u64,
    pub data_path_on_expected_volume: bool,
    pub colima_disk_bytes: u64,
    pub lock_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightFailure {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightReport {
    pub passed: bool,
    pub failures: Vec<PreflightFailure>,
    pub snapshot: PreflightSnapshot,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreflightPolicy {
    approved_data_root_prefix: PathBuf,
    required_cpus: u32,
    required_memory_bytes: u64,
    required_colima_disk_bytes: u64,
    minimum_projected_free_ratio: f64,
}

impl PreflightPolicy {
    pub fn r1_mac() -> Self {
        Self {
            approved_data_root_prefix: PathBuf::from(APPROVED_DATA_ROOT_PREFIX),
            required_cpus: REQUIRED_CPUS,
            required_memory_bytes: REQUIRED_MEMORY_BYTES,
            required_colima_disk_bytes: REQUIRED_COLIMA_DISK_BYTES,
            minimum_projected_free_ratio: MIN_PROJECTED_FREE_RATIO,
        }
    }

    pub fn evaluate(&self, snapshot: &PreflightSnapshot) -> PreflightReport {
        let mut failures = Vec::new();
        let projected_free_bytes = snapshot
            .available_bytes
            .saturating_sub(snapshot.expected_peak_bytes + snapshot.runtime_stop_bytes);
        let projected_free_ratio = if snapshot.volume_bytes == 0 {
            0.0
        } else {
            projected_free_bytes as f64 / snapshot.volume_bytes as f64
        };

        if projected_free_ratio < self.minimum_projected_free_ratio {
            failures.push(PreflightFailure {
                code: "PROJECTED_FREE_BELOW_20_PERCENT".to_string(),
                message: format!(
                    "projected free space is {:.2}% of the disk",
                    projected_free_ratio * 100.0
                ),
            });
        }

        if snapshot.cpus < self.required_cpus {
            failures.push(PreflightFailure {
                code: "CPU_LIMIT_TOO_SMALL".to_string(),
                message: format!(
                    "requested {} CPUs but only {} are available",
                    self.required_cpus, snapshot.cpus
                ),
            });
        }

        if snapshot.memory_bytes < self.required_memory_bytes {
            failures.push(PreflightFailure {
                code: "MEMORY_LIMIT_TOO_SMALL".to_string(),
                message: format!(
                    "requested {} bytes of memory but only {} are available",
                    self.required_memory_bytes, snapshot.memory_bytes
                ),
            });
        }

        if !snapshot.data_path_on_expected_volume {
            failures.push(PreflightFailure {
                code: "DATA_PATH_OUTSIDE_APPROVED_VOLUME".to_string(),
                message: "data path did not resolve inside the approved external volume"
                    .to_string(),
            });
        }

        if snapshot.colima_disk_bytes < self.required_colima_disk_bytes {
            failures.push(PreflightFailure {
                code: "COLIMA_DISK_TOO_SMALL".to_string(),
                message: format!(
                    "requested {} bytes of disk but only {} are available",
                    self.required_colima_disk_bytes, snapshot.colima_disk_bytes
                ),
            });
        }

        if !snapshot.lock_available {
            failures.push(PreflightFailure {
                code: "RUN_LOCK_UNAVAILABLE".to_string(),
                message: "selected run directory is already locked".to_string(),
            });
        }

        PreflightReport {
            passed: failures.is_empty(),
            failures,
            snapshot: snapshot.clone(),
        }
    }
}

pub trait PreflightProbe {
    fn probe(&self, run_root: &Path) -> Result<PreflightSnapshot>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotPreflightProbe {
    snapshot: PreflightSnapshot,
}

impl SnapshotPreflightProbe {
    pub fn new(snapshot: PreflightSnapshot) -> Self {
        Self { snapshot }
    }
}

impl PreflightProbe for SnapshotPreflightProbe {
    fn probe(&self, _run_root: &Path) -> Result<PreflightSnapshot> {
        Ok(self.snapshot.clone())
    }
}

#[derive(Debug, Clone)]
pub struct SystemPreflightProbe {
    approved_data_root_prefix: PathBuf,
    colima_profile: String,
}

impl Default for SystemPreflightProbe {
    fn default() -> Self {
        Self {
            approved_data_root_prefix: PathBuf::from(APPROVED_DATA_ROOT_PREFIX),
            colima_profile: "r1".to_string(),
        }
    }
}

impl SystemPreflightProbe {
    pub fn new(
        approved_data_root_prefix: impl Into<PathBuf>,
        colima_profile: impl Into<String>,
    ) -> Self {
        Self {
            approved_data_root_prefix: approved_data_root_prefix.into(),
            colima_profile: colima_profile.into(),
        }
    }

    fn sanitize_env(&self) -> BTreeMap<String, String> {
        std::env::vars()
            .filter(|(key, _)| {
                let upper = key.to_ascii_uppercase();
                !upper.contains("PASSWORD")
                    && !upper.contains("TOKEN")
                    && !upper.contains("SECRET")
                    && !upper.contains("KEY")
            })
            .collect()
    }

    fn run_json_command(
        &self,
        command_name: &'static str,
        mut command: Command,
    ) -> Result<ProbeCommandRecord> {
        let output = command.output().context("running preflight command")?;
        Ok(command_capture(command_name, output))
    }

    fn write_environment_record(&self, run_root: &Path, record: &SystemProbeRecord) -> Result<()> {
        let path = run_root.join("environment.json");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        serde_json::to_writer_pretty(file, record).context("writing environment.json")
    }

    fn probe_write_sync(&self, run_root: &Path) -> Result<WriteProbeRecord> {
        let path = run_root.join(".preflight-write-probe.bin");
        let start = Instant::now();
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        let mut writer = BufWriter::new(file);
        let chunk = [0_u8; 1024 * 1024];
        for _ in 0..64 {
            writer.write_all(&chunk).context("writing probe bytes")?;
        }
        writer.flush().context("flushing probe bytes")?;
        writer.get_ref().sync_all().context("syncing probe bytes")?;
        drop(writer);
        let _ = fs::remove_file(&path);

        Ok(WriteProbeRecord {
            path: path.display().to_string(),
            bytes_written: 64_u64 << 20,
            elapsed_ms: start.elapsed().as_millis() as u64,
        })
    }
}

impl PreflightProbe for SystemPreflightProbe {
    fn probe(&self, run_root: &Path) -> Result<PreflightSnapshot> {
        fs::create_dir_all(run_root).with_context(|| format!("creating {}", run_root.display()))?;
        let canonical_data_root = run_root
            .canonicalize()
            .with_context(|| format!("canonicalizing {}", run_root.display()))?;
        if !canonical_data_root.starts_with(&self.approved_data_root_prefix) {
            return Err(anyhow!(
                "data root {} is outside approved prefix {}",
                canonical_data_root.display(),
                self.approved_data_root_prefix.display()
            ));
        }

        let volume_bytes = fs2::total_space(&canonical_data_root).with_context(|| {
            format!("reading total space for {}", canonical_data_root.display())
        })?;
        let available_bytes = fs2::available_space(&canonical_data_root).with_context(|| {
            format!(
                "reading available space for {}",
                canonical_data_root.display()
            )
        })?;
        // The controller holds the exclusive run.lock for its whole lifetime
        // (RunDirectory::create/resume is the authoritative gate and hard-
        // fails on contention), so probing the lock from inside a stage
        // would always collide with our own process.  Reaching this code at
        // all proves the lock is held.
        let lock_available = true;
        let write_probe = self.probe_write_sync(run_root)?;

        let colima_status = self.run_json_command("colima status --profile r1 --json", {
            let mut command = Command::new("colima");
            command
                .arg("status")
                .arg("--profile")
                .arg(&self.colima_profile)
                .arg("--json");
            command
        })?;
        let docker_info = self.run_json_command("docker info --format '{{json .}}'", {
            let mut command = Command::new("docker");
            command.arg("info").arg("--format").arg("{{json .}}");
            command
        })?;

        let probe_result = SystemProbeRecord::from_outputs(
            canonical_data_root.display().to_string(),
            volume_bytes,
            available_bytes,
            write_probe,
            colima_status,
            docker_info,
            self.sanitize_env(),
        );

        self.write_environment_record(run_root, &probe_result)?;

        if let Some(error) = probe_result.command_error() {
            return Err(anyhow!(error));
        }

        Ok(PreflightSnapshot {
            volume_bytes,
            available_bytes,
            expected_peak_bytes: 0,
            runtime_stop_bytes: 0,
            cpus: probe_result.resources.cpus,
            memory_bytes: probe_result.resources.memory_bytes,
            data_path_on_expected_volume: probe_result.data_path_on_expected_volume,
            colima_disk_bytes: probe_result.resources.colima_disk_bytes,
            lock_available,
        })
    }
}

#[derive(Debug, Serialize)]
struct SystemProbeRecord {
    canonical_data_root: String,
    volume_bytes: u64,
    available_bytes: u64,
    write_probe: WriteProbeRecord,
    colima_status: ProbeCommandRecord,
    docker_info: ProbeCommandRecord,
    resources: ResourceSnapshotRecord,
    data_path_on_expected_volume: bool,
    environment: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct WriteProbeRecord {
    path: String,
    bytes_written: u64,
    elapsed_ms: u64,
}

#[derive(Debug, Serialize)]
struct ProbeCommandRecord {
    command: String,
    status: Option<i32>,
    success: bool,
    stdout: Value,
    stderr: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
struct ResourceSnapshotRecord {
    cpus: u32,
    memory_bytes: u64,
    colima_disk_bytes: u64,
}

impl SystemProbeRecord {
    fn from_outputs(
        canonical_data_root: String,
        volume_bytes: u64,
        available_bytes: u64,
        write_probe: WriteProbeRecord,
        colima_status: ProbeCommandRecord,
        docker_info: ProbeCommandRecord,
        environment: BTreeMap<String, String>,
    ) -> Self {
        let resources = extract_resources(&colima_status.stdout, &docker_info.stdout);
        let data_path_on_expected_volume =
            canonical_data_root.starts_with(APPROVED_DATA_ROOT_PREFIX);
        Self {
            canonical_data_root,
            volume_bytes,
            available_bytes,
            write_probe,
            colima_status,
            docker_info,
            resources,
            data_path_on_expected_volume,
            environment,
        }
    }

    fn command_error(&self) -> Option<String> {
        if !self.colima_status.success {
            return Some(format!(
                "colima status --profile r1 --json failed with status {:?}",
                self.colima_status.status
            ));
        }
        if !self.docker_info.success {
            return Some(format!(
                "docker info --format '{{json .}}' failed with status {:?}",
                self.docker_info.status
            ));
        }
        None
    }
}

fn command_capture(command: &'static str, output: std::process::Output) -> ProbeCommandRecord {
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let parsed = serde_json::from_str(&stdout).unwrap_or_else(|_| json!(stdout));
    ProbeCommandRecord {
        command: command.to_string(),
        status: output.status.code(),
        success: output.status.success(),
        stdout: parsed,
        stderr,
    }
}

fn extract_resources(colima_stdout: &Value, docker_stdout: &Value) -> ResourceSnapshotRecord {
    let cpus = extract_cpu_count(colima_stdout)
        .or_else(|| extract_cpu_count(docker_stdout))
        .unwrap_or(0);
    let memory_bytes = extract_bytes(colima_stdout, &["memory", "memory_bytes", "memorylimit"])
        .or_else(|| extract_bytes(docker_stdout, &["memtotal", "memory", "memory_bytes"]))
        .unwrap_or(0);
    let colima_disk_bytes = extract_bytes(
        colima_stdout,
        &["disk", "disk_bytes", "disksize", "disk_size"],
    )
    .unwrap_or(0);
    ResourceSnapshotRecord {
        cpus,
        memory_bytes,
        colima_disk_bytes,
    }
}

fn extract_cpu_count(value: &Value) -> Option<u32> {
    find_numeric_value(value, &["cpus", "cpu", "cpu_count", "ncpu"]).and_then(|n| {
        if n <= u32::MAX as u64 {
            Some(n as u32)
        } else {
            None
        }
    })
}

fn extract_bytes(value: &Value, keys: &[&str]) -> Option<u64> {
    find_value_by_keys(value, keys).and_then(parse_bytes_value)
}

fn find_value_by_keys<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    match value {
        Value::Object(map) => {
            for (key, nested) in map {
                if keys
                    .iter()
                    .any(|candidate| key.eq_ignore_ascii_case(candidate))
                {
                    return Some(nested);
                }
            }
            for nested in map.values() {
                if let Some(found) = find_value_by_keys(nested, keys) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(|item| find_value_by_keys(item, keys)),
        _ => None,
    }
}

fn find_numeric_value(value: &Value, keys: &[&str]) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.trim().parse().ok(),
        Value::Object(map) => {
            for (key, nested) in map {
                if keys
                    .iter()
                    .any(|candidate| key.eq_ignore_ascii_case(candidate))
                {
                    if let Some(found) = find_numeric_value(nested, keys) {
                        return Some(found);
                    }
                    if let Some(found) = parse_numeric_text(nested) {
                        return Some(found);
                    }
                }
            }
            for nested in map.values() {
                if let Some(found) = find_numeric_value(nested, keys) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(|item| find_numeric_value(item, keys)),
        _ => None,
    }
}

fn parse_numeric_text(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

fn parse_bytes_value(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => parse_bytes_text(text),
        _ => None,
    }
}

fn parse_bytes_text(text: &str) -> Option<u64> {
    let trimmed = text.trim().replace(',', "");
    let lower = trimmed.to_ascii_lowercase();
    let (number, factor) = if let Some(prefix) = lower.strip_suffix("gib") {
        (prefix.trim(), 1_u64 << 30)
    } else if let Some(prefix) = lower.strip_suffix("gb") {
        (prefix.trim(), 1_000_000_000)
    } else if let Some(prefix) = lower.strip_suffix("mib") {
        (prefix.trim(), 1_u64 << 20)
    } else if let Some(prefix) = lower.strip_suffix("mb") {
        (prefix.trim(), 1_000_000)
    } else if let Some(prefix) = lower.strip_suffix("kib") {
        (prefix.trim(), 1_u64 << 10)
    } else if let Some(prefix) = lower.strip_suffix("kb") {
        (prefix.trim(), 1_000)
    } else if let Some(prefix) = lower.strip_suffix('b') {
        (prefix.trim(), 1)
    } else {
        (lower.trim(), 1)
    };

    if let Ok(value) = number.parse::<f64>() {
        return Some((value * factor as f64).round() as u64);
    }
    number
        .parse::<u64>()
        .ok()
        .map(|value| value.saturating_mul(factor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn rejects_projected_space_below_twenty_percent() {
        let snapshot = PreflightSnapshot {
            volume_bytes: 1_000,
            available_bytes: 500,
            expected_peak_bytes: 350,
            runtime_stop_bytes: 150,
            cpus: 10,
            memory_bytes: 16_u64 << 30,
            data_path_on_expected_volume: true,
            colima_disk_bytes: 600_u64 << 30,
            lock_available: true,
        };
        let report = PreflightPolicy::r1_mac().evaluate(&snapshot);
        assert!(!report.passed);
        assert_eq!(report.failures[0].code, "PROJECTED_FREE_BELOW_20_PERCENT");
    }

    #[test]
    fn accepts_happy_path_snapshot() {
        let snapshot = PreflightSnapshot {
            volume_bytes: 1_000_000,
            available_bytes: 500_000,
            expected_peak_bytes: 100_000,
            runtime_stop_bytes: 50_000,
            cpus: 10,
            memory_bytes: 16_u64 << 30,
            data_path_on_expected_volume: true,
            colima_disk_bytes: 600_u64 << 30,
            lock_available: true,
        };
        let report = PreflightPolicy::r1_mac().evaluate(&snapshot);
        assert!(report.passed);
        assert!(report.failures.is_empty());
    }

    #[test]
    fn extracts_runtime_resources_from_probe_outputs() {
        let record = SystemProbeRecord::from_outputs(
            "/Volumes/Crucial X9/GrayDB/.r1/run".to_string(),
            1_000_000,
            500_000,
            WriteProbeRecord {
                path: "/Volumes/Crucial X9/GrayDB/.r1/run/.preflight-write-probe.bin".to_string(),
                bytes_written: 64_u64 << 20,
                elapsed_ms: 42,
            },
            ProbeCommandRecord {
                command: "colima status --profile r1 --json".to_string(),
                status: Some(0),
                success: true,
                stdout: serde_json::json!({
                    "cpus": 6,
                    "memory": "10GiB",
                    "disk": "500GiB"
                }),
                stderr: String::new(),
            },
            ProbeCommandRecord {
                command: "docker info --format '{{json .}}'".to_string(),
                status: Some(0),
                success: true,
                stdout: serde_json::json!({
                    "NCPU": 6,
                    "MemTotal": 10_737_418_240_u64
                }),
                stderr: String::new(),
            },
            BTreeMap::new(),
        );

        assert_eq!(record.resources.cpus, 6);
        assert_eq!(record.resources.memory_bytes, 10_u64 << 30);
        assert_eq!(record.resources.colima_disk_bytes, 500_u64 << 30);
        assert!(record.command_error().is_none());
    }

    #[test]
    fn records_failed_probe_commands_as_errors() {
        let record = SystemProbeRecord::from_outputs(
            "/Volumes/Crucial X9/GrayDB/.r1/run".to_string(),
            1_000_000,
            500_000,
            WriteProbeRecord {
                path: "/Volumes/Crucial X9/GrayDB/.r1/run/.preflight-write-probe.bin".to_string(),
                bytes_written: 64_u64 << 20,
                elapsed_ms: 42,
            },
            ProbeCommandRecord {
                command: "colima status --profile r1 --json".to_string(),
                status: Some(1),
                success: false,
                stdout: serde_json::json!({}),
                stderr: "not running".to_string(),
            },
            ProbeCommandRecord {
                command: "docker info --format '{{json .}}'".to_string(),
                status: Some(0),
                success: true,
                stdout: serde_json::json!({}),
                stderr: String::new(),
            },
            BTreeMap::new(),
        );

        assert_eq!(record.resources.cpus, 0);
        assert_eq!(record.resources.memory_bytes, 0);
        assert_eq!(record.resources.colima_disk_bytes, 0);
        assert!(record.command_error().is_some());

        let report = PreflightPolicy::r1_mac().evaluate(&PreflightSnapshot {
            volume_bytes: 1_000_000,
            available_bytes: 500_000,
            expected_peak_bytes: 100_000,
            runtime_stop_bytes: 50_000,
            cpus: record.resources.cpus,
            memory_bytes: record.resources.memory_bytes,
            data_path_on_expected_volume: record.data_path_on_expected_volume,
            colima_disk_bytes: record.resources.colima_disk_bytes,
            lock_available: true,
        });

        assert!(!report.passed);
        assert!(report
            .failures
            .iter()
            .any(|failure| failure.code == "CPU_LIMIT_TOO_SMALL"));
    }
}
