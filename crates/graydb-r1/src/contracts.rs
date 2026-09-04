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
    pub insert_rows_pct: u8,
    pub update_rows_pct: u8,
    pub delete_rows_pct: u8,
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
        assert_eq!(catalog.benchmark_id, "R1-P1-v1");
        assert_eq!(catalog.seed, 20260901);
        assert_eq!(catalog.insert_rows_pct, 90);
        assert_eq!(catalog.update_rows_pct, 8);
        assert_eq!(catalog.delete_rows_pct, 2);
        assert_eq!(catalog.freshness_p99_limit_ms, 1_000);
        assert_eq!(catalog.backlog_limit_bytes, 10_737_418_240);
        assert_eq!(catalog.minimum_query_samples, 30);
        assert_eq!(catalog.fixed_rates, vec![300, 1000]);
        assert_eq!(
            catalog.search_rates,
            vec![2000, 4000, 8000, 16000, 32000, 64000]
        );

        let expect = [
            (
                ScaleProfile::MacSmoke,
                1_u64 << 30,
                1,
                60,
                120,
                120,
                120,
                2_000,
            ),
            (
                ScaleProfile::MacCorrectness,
                10_u64 << 30,
                1,
                120,
                300,
                300,
                180,
                4_000,
            ),
            (
                ScaleProfile::MacValidation,
                50_u64 << 30,
                3,
                300,
                600,
                600,
                300,
                8_000,
            ),
            (
                ScaleProfile::MacStress,
                100_u64 << 30,
                3,
                600,
                900,
                1200,
                600,
                16_000,
            ),
            (
                ScaleProfile::MacCeiling,
                200_u64 << 30,
                3,
                600,
                900,
                1200,
                600,
                16_000,
            ),
            (
                ScaleProfile::AwsPhase1,
                1_u64 << 40,
                3,
                900,
                1_800,
                1_800,
                900,
                64_000,
            ),
        ];

        for (
            profile,
            minimum_bytes,
            repetitions,
            warmup_secs,
            quiet_secs,
            fixed_rate_secs,
            search_step_secs,
            maximum_rate,
        ) in expect
        {
            let spec = catalog.get(profile).unwrap();
            assert_eq!(spec.minimum_bytes, minimum_bytes, "{profile:?}");
            assert_eq!(spec.repetitions, repetitions, "{profile:?}");
            assert_eq!(spec.warmup_secs, warmup_secs, "{profile:?}");
            assert_eq!(spec.quiet_secs, quiet_secs, "{profile:?}");
            assert_eq!(spec.fixed_rate_secs, fixed_rate_secs, "{profile:?}");
            assert_eq!(spec.search_step_secs, search_step_secs, "{profile:?}");
            assert_eq!(spec.maximum_rate, maximum_rate, "{profile:?}");
        }
    }
}
