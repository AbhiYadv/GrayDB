use anyhow::{Context, Result};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ValueEnum,
)]
#[serde(rename_all = "kebab-case")]
pub enum ScaleProfile {
    MacSmoke,
    MacCorrectness,
    MacValidation,
    MacStress,
    MacCeiling,
    AwsPhase1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum RunMode {
    Correctness,
    Isolated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum EngineKind {
    Graydb,
    Clickhouse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalCheckpoint {
    pub sequence: u64,
    pub source_lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileSpec {
    pub minimum_bytes: u64,
    pub repetitions: u8,
    pub warmup_secs: u64,
    pub quiet_secs: u64,
    pub fixed_rate_secs: u64,
    pub search_step_secs: u64,
    pub maximum_rate: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileCatalog {
    pub benchmark_id: String,
    pub seed: u64,
    pub freshness_p99_limit_ms: u64,
    pub backlog_limit_bytes: u64,
    pub minimum_query_samples: u64,
    pub fixed_rates: Vec<u64>,
    pub search_rates: Vec<u64>,
    pub profiles: BTreeMap<ScaleProfile, ProfileSpec>,
}

impl ProfileCatalog {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading profile catalog from {}", path.display()))?;
        let catalog = toml::from_str(&raw)
            .with_context(|| format!("parsing profile catalog from {}", path.display()))?;
        Ok(catalog)
    }

    pub fn get(&self, profile: ScaleProfile) -> Option<&ProfileSpec> {
        self.profiles.get(&profile)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    pub benchmark_id: String,
    pub seed: u64,
    pub profile: ScaleProfile,
    pub spec: ProfileSpec,
    pub run_mode: RunMode,
    pub engine: EngineKind,
    pub checkpoint: Option<LogicalCheckpoint>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn repo_file(relative: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(relative)
    }

    #[test]
    fn profile_catalog_matches_r1_p1_v1() {
        let catalog = ProfileCatalog::load(repo_file("bench/r1/profiles.toml")).unwrap();
        let smoke = catalog.get(ScaleProfile::MacSmoke).unwrap();
        assert_eq!(smoke.minimum_bytes, 1_u64 << 30);
        assert_eq!(smoke.repetitions, 1);
        assert_eq!(smoke.warmup_secs, 60);
        assert_eq!(smoke.quiet_secs, 120);
        assert_eq!(smoke.fixed_rate_secs, 120);
        assert_eq!(smoke.search_step_secs, 120);
        assert_eq!(smoke.maximum_rate, 2_000);

        let aws = catalog.get(ScaleProfile::AwsPhase1).unwrap();
        assert_eq!(aws.minimum_bytes, 1_u64 << 40);
        assert_eq!(aws.repetitions, 3);
        assert_eq!(aws.warmup_secs, 900);
        assert_eq!(aws.quiet_secs, 1_800);
        assert_eq!(aws.fixed_rate_secs, 1_800);
        assert_eq!(aws.search_step_secs, 900);
        assert_eq!(aws.maximum_rate, 64_000);
    }
}
