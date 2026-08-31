//! graydb.toml loader. Every spec magic number lives in the file, not in code.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub source: Source,
    pub initial_load: InitialLoad,
    pub wal_budget: WalBudget,
    pub consistency: Consistency,
    pub retention: Retention,
    pub storage: Storage,
    pub log: LogCfg,
    pub columnar: ColumnarCfg,
    pub search: SearchCfg,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnarCfg {
    pub flush_rows: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchCfg {
    pub commit_batch_txns: u64,
    #[serde(default)]
    pub indexes: Vec<SearchIndexCfg>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchIndexCfg {
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Source {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: String,
    pub schema: String,
    pub publication: String,
    pub slot: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InitialLoad {
    pub copy_streams: usize,
    pub read_budget_pct: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalBudget {
    pub bytes_cap: u64,
    pub time_cap_secs: u64,
    pub warn_fraction: f64,
    pub shed_fraction: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogCfg {
    pub segment_max_bytes: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Consistency {
    pub session_default: String,
    pub heartbeat_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Retention {
    pub historical_window_days: u32,
    pub base_refresh_fraction: f64,
    pub base_refresh_days: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Storage {
    pub data_dir: PathBuf,
}

impl Config {
    /// Load graydb.toml. Search order, first hit wins:
    ///   1. $GRAYDB_CONFIG (explicit path)
    ///   2. cwd, then each parent — so `cargo run` works from anywhere in the workspace
    ///   3. the directory holding the running executable, and its parent — so a
    ///      distributed folder (binary + graydb.toml side by side) works from any cwd
    pub fn load() -> Result<Self> {
        if let Ok(explicit) = std::env::var("GRAYDB_CONFIG") {
            let p = PathBuf::from(explicit);
            anyhow::ensure!(p.is_file(), "GRAYDB_CONFIG points at {}, which is not a file", p.display());
            return Self::load_from(&p);
        }
        let path = find_config().context(
            "graydb.toml not found (searched cwd upward, then next to the executable; \
             set GRAYDB_CONFIG=<path> to be explicit)",
        )?;
        Self::load_from(&path)
    }

    pub fn load_from(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        let mut cfg: Config = toml::from_str(&raw).context("parsing graydb.toml")?;
        // A relative data_dir is relative to the CONFIG, not the cwd: a distributed
        // folder (binary + graydb.toml + data/) then stays self-contained wherever
        // it is launched from.
        if cfg.storage.data_dir.is_relative() {
            if let Some(base) = path.parent() {
                cfg.storage.data_dir = base.join(&cfg.storage.data_dir);
            }
        }
        // Env overrides (D-006: lets `just demo-sp1-pg16` retarget without editing config).
        if let Ok(host) = std::env::var("GRAYDB_SOURCE_HOST") {
            cfg.source.host = host;
        }
        if let Ok(port) = std::env::var("GRAYDB_SOURCE_PORT") {
            cfg.source.port = port.parse().context("GRAYDB_SOURCE_PORT not a port")?;
        }
        if let Ok(pw) = std::env::var("GRAYDB_SOURCE_PASSWORD") {
            cfg.source.password = pw;
        }
        Ok(cfg)
    }

    /// tokio-postgres config for ordinary SQL sessions.
    pub fn pg_config(&self) -> tokio_postgres::Config {
        let mut c = tokio_postgres::Config::new();
        c.host(&self.source.host)
            .port(self.source.port)
            .dbname(&self.source.dbname)
            .user(&self.source.user)
            .password(&self.source.password)
            .application_name("graydb");
        c
    }

    /// Spawn a connection and drive it in the background; returns the client.
    pub async fn connect(&self) -> Result<tokio_postgres::Client> {
        let (client, connection) = self
            .pg_config()
            .connect(tokio_postgres::NoTls)
            .await
            .with_context(|| {
                format!(
                    "connecting to {}:{}/{}",
                    self.source.host, self.source.port, self.source.dbname
                )
            })?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "postgres connection task ended with error");
            }
        });
        Ok(client)
    }
}

fn find_config() -> Option<PathBuf> {
    if let Ok(mut dir) = std::env::current_dir() {
        loop {
            let candidate = dir.join("graydb.toml");
            if candidate.is_file() {
                return Some(candidate);
            }
            if !dir.pop() {
                break;
            }
        }
    }
    // Distributed layout: graydb.toml sits beside the binary (or one level up).
    let exe = std::env::current_exe().ok()?;
    let exe_dir = exe.parent()?;
    for dir in [exe_dir, exe_dir.parent()?] {
        let candidate = dir.join("graydb.toml");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}
