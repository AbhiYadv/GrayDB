use anyhow::{anyhow, Context, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventLevel {
    Info,
    Warning,
    Error,
    Stage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub monotonic_ns: u128,
    pub wall_unix_ms: u128,
    pub level: EventLevel,
    pub stage: String,
    pub operation: String,
    pub message: String,
    pub fields: BTreeMap<String, Value>,
    #[serde(skip)]
    secrets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRender {
    pub human: String,
    pub json: String,
}

impl Event {
    pub fn new(
        level: EventLevel,
        stage: impl Into<String>,
        operation: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            monotonic_ns: monotonic_time_ns(),
            wall_unix_ms: wall_time_ms(),
            level,
            stage: stage.into(),
            operation: operation.into(),
            message: message.into(),
            fields: BTreeMap::new(),
            secrets: Vec::new(),
        }
    }

    pub fn info(stage: impl Into<String>, message: impl Into<String>) -> Self {
        let stage = stage.into();
        Self::new(EventLevel::Info, stage.clone(), stage, message)
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    pub fn with_secret(mut self, secret: impl Into<String>) -> Self {
        let secret = secret.into();
        if !secret.is_empty() {
            self.secrets.push(secret);
        }
        self
    }

    pub fn render_redacted(&self) -> EventRender {
        let mut redacted = self.clone();
        redact_event(&mut redacted);

        let json = serde_json::to_string(&redacted).expect("event serialization");
        let human = format!(
            "{:?} stage={} operation={} message={} fields={}",
            redacted.level,
            redacted.stage,
            redacted.operation,
            redacted.message,
            serde_json::to_string(&redacted.fields).expect("field serialization"),
        );

        EventRender { human, json }
    }
}

#[derive(Debug)]
pub struct RunDirectory {
    root: PathBuf,
    lock: File,
}

impl RunDirectory {
    pub fn create(root: impl AsRef<Path>, run_id: impl AsRef<str>) -> Result<Self> {
        let root = root.as_ref().join(run_id.as_ref());
        fs::create_dir_all(&root)
            .with_context(|| format!("creating run directory {}", root.display()))?;

        for relative in [
            "configs",
            "ddl",
            "queries",
            "explain",
            "metrics",
            "correctness",
            "failure-events",
        ] {
            fs::create_dir_all(root.join(relative))
                .with_context(|| format!("creating artifact subdirectory {relative}"))?;
        }

        for relative in [
            "run.log",
            "events.jsonl",
            "dataset-manifest.json",
            "workload-manifest.json",
            "environment.json",
            "result.json",
            "result.md",
            "SHA256SUMS",
        ] {
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(root.join(relative))
                .with_context(|| format!("creating artifact file {relative}"))?;
        }

        let lock_path = root.join("run.lock");
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("opening {}", lock_path.display()))?;
        lock.try_lock_exclusive()
            .with_context(|| format!("locking {}", lock_path.display()))?;

        Ok(Self { root, lock })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn path(&self, relative: impl AsRef<Path>) -> PathBuf {
        self.root.join(relative)
    }

    pub fn event_sink(&self) -> Result<EventSink> {
        EventSink::open(self)
    }
}

impl Drop for RunDirectory {
    fn drop(&mut self) {
        let _ = self.lock.unlock();
    }
}

#[derive(Debug)]
pub struct EventSink {
    run_log: BufWriter<File>,
    events_jsonl: BufWriter<File>,
}

impl EventSink {
    pub fn open(run_directory: &RunDirectory) -> Result<Self> {
        let run_log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(run_directory.path("run.log"))
            .with_context(|| "opening run.log".to_string())?;
        let events_jsonl = OpenOptions::new()
            .create(true)
            .append(true)
            .open(run_directory.path("events.jsonl"))
            .with_context(|| "opening events.jsonl".to_string())?;
        Ok(Self {
            run_log: BufWriter::new(run_log),
            events_jsonl: BufWriter::new(events_jsonl),
        })
    }

    pub fn emit(&mut self, event: &Event) -> Result<()> {
        let rendered = event.render_redacted();
        writeln!(self.run_log, "{}", rendered.human).context("writing run.log")?;
        writeln!(self.events_jsonl, "{}", rendered.json).context("writing events.jsonl")?;
        self.run_log.flush().context("flushing run.log")?;
        self.events_jsonl.flush().context("flushing events.jsonl")?;
        Ok(())
    }
}

pub fn sha256_tree(root: impl AsRef<Path>) -> Result<PathBuf> {
    let root = root.as_ref();
    let mut files = Vec::new();
    collect_files(root, root, &mut files)?;
    files.sort_by(|a, b| a.0.cmp(&b.0));

    let sums_path = root.join("SHA256SUMS");
    let mut output = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&sums_path)
            .with_context(|| format!("opening {}", sums_path.display()))?,
    );

    for (relative, absolute) in files {
        let bytes =
            fs::read(&absolute).with_context(|| format!("reading {}", absolute.display()))?;
        let digest = Sha256::digest(&bytes);
        writeln!(output, "{:x}  {}", digest, relative.to_string_lossy())
            .with_context(|| format!("writing checksum for {}", relative.display()))?;
    }
    output.flush().context("flushing SHA256SUMS")?;
    Ok(sums_path)
}

fn collect_files(root: &Path, current: &Path, out: &mut Vec<(PathBuf, PathBuf)>) -> Result<()> {
    for entry in fs::read_dir(current).with_context(|| format!("reading {}", current.display()))? {
        let entry = entry.with_context(|| format!("reading {}", current.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("inspecting {}", path.display()))?;
        if file_type.is_dir() {
            collect_files(root, &path, out)?;
            continue;
        }

        let relative = path
            .strip_prefix(root)
            .map(Path::to_path_buf)
            .map_err(|_| anyhow!("path {} is outside {}", path.display(), root.display()))?;
        if matches!(
            relative.file_name().and_then(|name| name.to_str()),
            Some("run.lock" | "SHA256SUMS")
        ) {
            continue;
        }
        out.push((relative, path));
    }
    Ok(())
}

fn redact_event(event: &mut Event) {
    let secrets = event.secrets.clone();
    if secrets.is_empty() {
        return;
    }

    event.message = redact_string(&event.message, &secrets);
    event.stage = redact_string(&event.stage, &secrets);
    event.operation = redact_string(&event.operation, &secrets);
    for value in event.fields.values_mut() {
        redact_value(value, &secrets);
    }
}

fn redact_value(value: &mut Value, secrets: &[String]) {
    match value {
        Value::String(text) => {
            *text = redact_string(text, secrets);
        }
        Value::Array(values) => {
            for value in values {
                redact_value(value, secrets);
            }
        }
        Value::Object(map) => {
            for value in map.values_mut() {
                redact_value(value, secrets);
            }
        }
        _ => {}
    }
}

fn redact_string(text: &str, secrets: &[String]) -> String {
    let mut redacted = text.to_string();
    for secret in secrets {
        redacted = redacted.replace(secret, "[REDACTED]");
    }
    redacted
}

fn monotonic_time_ns() -> u128 {
    use std::time::Instant;
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos()
}

fn wall_time_ms() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn redacts_credentials_from_both_log_formats() {
        let event =
            Event::info("connect", "postgres://postgres:hunter2@pg/appdb").with_secret("hunter2");
        let rendered = event.render_redacted();
        assert!(!rendered.human.contains("hunter2"));
        assert!(!rendered.json.contains("hunter2"));
        assert!(rendered.human.contains("[REDACTED]"));
    }

    #[test]
    fn sha256_tree_skips_lock_and_checksum_files() {
        let root = tempdir().unwrap();
        fs::write(root.path().join("a.txt"), b"alpha").unwrap();
        fs::create_dir_all(root.path().join("nested")).unwrap();
        fs::write(root.path().join("nested").join("b.txt"), b"beta").unwrap();
        fs::write(root.path().join("run.lock"), b"ignored").unwrap();
        fs::write(root.path().join("SHA256SUMS"), b"ignored").unwrap();

        let sums = sha256_tree(root.path()).unwrap();
        let rendered = fs::read_to_string(sums).unwrap();
        assert!(rendered.contains("a.txt"));
        assert!(rendered.contains("nested/b.txt"));
        assert!(!rendered.contains("run.lock"));
        assert!(!rendered.contains("SHA256SUMS"));
    }
}
