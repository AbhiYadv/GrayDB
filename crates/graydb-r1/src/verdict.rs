//! Validity and winner rules from spec section 16. One correctness failure
//! voids every performance number in the run; invalid runs never carry winner
//! language into reports.

use crate::query::QueryId;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunInvalidation {
    DatasetHashMismatch,
    WorkloadHashMismatch,
    MissingSequence(u64),
    DuplicateSequence(u64),
    StateChangingReorder { before: u64, after: u64 },
    StaleResult { target_lsn: u64, visible_lsn: u64 },
    ResultDigestMismatch { query: QueryId, checkpoint: u64 },
    FreshnessP99Exceeded { limit_ms: u64, actual_ms: u64 },
    SourceRateMissed { target: u64, achieved: u64 },
    ResourceSafetyGate(String),
    UnexpectedProcessExit(String),
    MissingArtifact(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CellVerdict {
    GrayDbWin,
    ClickHouseWin,
    Tie,
    ConflictingTail,
}

/// Cell rule: a win requires p95 at least 5% lower AND p99 no higher.
/// A tie requires both differences below 5%. Everything else is a
/// conflicting-tail cell.
pub fn evaluate_cell(
    graydb_p95_ns: u128,
    graydb_p99_ns: u128,
    clickhouse_p95_ns: u128,
    clickhouse_p99_ns: u128,
) -> CellVerdict {
    if at_least_percent_lower(graydb_p95_ns, clickhouse_p95_ns, 5)
        && graydb_p99_ns <= clickhouse_p99_ns
    {
        CellVerdict::GrayDbWin
    } else if at_least_percent_lower(clickhouse_p95_ns, graydb_p95_ns, 5)
        && clickhouse_p99_ns <= graydb_p99_ns
    {
        CellVerdict::ClickHouseWin
    } else if differs_by_less_than_percent(graydb_p95_ns, clickhouse_p95_ns, 5)
        && differs_by_less_than_percent(graydb_p99_ns, clickhouse_p99_ns, 5)
    {
        CellVerdict::Tie
    } else {
        CellVerdict::ConflictingTail
    }
}

fn at_least_percent_lower(candidate: u128, baseline: u128, percent: u128) -> bool {
    baseline != 0 && candidate.saturating_mul(100) <= baseline.saturating_mul(100 - percent)
}

fn differs_by_less_than_percent(a: u128, b: u128, percent: u128) -> bool {
    if a == b {
        return true;
    }
    a.abs_diff(b).saturating_mul(100) < a.min(b).saturating_mul(percent)
}

/// The exact overall conclusion rule from spec section 16: at 1,000 rows/s and
/// the highest common sustainable rate, GrayDB must win at least four of Q1-Q5,
/// lose none, hold a geometric-mean p95 at least 10% lower, and a geometric-mean
/// p99 churn ratio at least 20% lower.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Scorecard {
    pub cells: BTreeMap<String, CellVerdict>,
    /// GrayDB / ClickHouse geometric-mean p95 ratio; lower is better.
    pub geometric_p95_ratio: f64,
    /// GrayDB / ClickHouse geometric-mean p99 churn ratio (CDC p99 / quiet p99).
    pub churn_ratio: f64,
}

/// The required two-stage evidence for the only comparative conclusion the R1
/// spec permits. `required_rates` is always the 1,000 rows/s stage plus the
/// maximum rate both engines completed; when 1,000 is itself the maximum it is
/// represented once.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WinnerEvaluation {
    pub required_rates: Vec<u64>,
    pub passed_rates: BTreeMap<u64, bool>,
}

impl WinnerEvaluation {
    pub fn graydb_beat_clickhouse(&self) -> bool {
        !self.required_rates.is_empty()
            && self
                .required_rates
                .iter()
                .all(|rate| self.passed_rates.get(rate) == Some(&true))
    }
}

impl Scorecard {
    /// Evaluates the exact two-rate winner rule. Supplying a rate map without
    /// 1,000 rows/s is insufficient, and a failure at either 1,000 or the
    /// highest common sustainable rate suppresses winner language.
    pub fn evaluate(rate_scorecards: &BTreeMap<u64, Scorecard>) -> WinnerEvaluation {
        let Some((&highest_common_rate, _)) = rate_scorecards.last_key_value() else {
            return WinnerEvaluation {
                required_rates: Vec::new(),
                passed_rates: BTreeMap::new(),
            };
        };
        let mut required_rates = vec![1_000];
        if highest_common_rate != 1_000 {
            required_rates.push(highest_common_rate);
        }
        let passed_rates = required_rates
            .iter()
            .map(|rate| {
                (
                    *rate,
                    rate_scorecards
                        .get(rate)
                        .map(Scorecard::graydb_beat_clickhouse)
                        .unwrap_or(false),
                )
            })
            .collect();
        WinnerEvaluation {
            required_rates,
            passed_rates,
        }
    }

    pub fn graydb_beat_clickhouse(&self) -> bool {
        const REQUIRED_QUERIES: [&str; 5] = ["q1", "q2", "q3", "q4", "q5"];
        if self.cells.len() != REQUIRED_QUERIES.len()
            || REQUIRED_QUERIES
                .iter()
                .any(|query| !self.cells.contains_key(*query))
        {
            return false;
        }
        let wins = self
            .cells
            .values()
            .filter(|c| **c == CellVerdict::GrayDbWin)
            .count();
        let losses = self
            .cells
            .values()
            .filter(|c| **c == CellVerdict::ClickHouseWin)
            .count();
        wins >= 4 && losses == 0 && self.geometric_p95_ratio <= 0.90 && self.churn_ratio <= 0.80
    }

    pub fn wins(&self) -> usize {
        self.cells
            .values()
            .filter(|c| **c == CellVerdict::GrayDbWin)
            .count()
    }

    pub fn losses(&self) -> usize {
        self.cells
            .values()
            .filter(|c| **c == CellVerdict::ClickHouseWin)
            .count()
    }

    pub fn ties(&self) -> usize {
        self.cells
            .values()
            .filter(|c| **c == CellVerdict::Tie)
            .count()
    }

    pub fn conflicting(&self) -> usize {
        self.cells
            .values()
            .filter(|c| **c == CellVerdict::ConflictingTail)
            .count()
    }
}

// --- Compose contract parsing (used by the Task 11 compose_contract test) ---

#[derive(Debug, Clone, Deserialize)]
pub struct ComposeService {
    #[serde(default)]
    pub healthcheck: Option<serde_yaml::Value>,
    #[serde(default, alias = "mem_limit")]
    pub memory_limit: Option<String>,
    #[serde(default)]
    pub volumes: Option<Vec<String>>,
}

impl ComposeService {
    /// Normalizes Compose memory limits like `3g` / `4096m` to bytes.
    pub fn memory_limit_bytes(&self) -> Option<u64> {
        let raw = self.memory_limit.as_ref()?;
        let raw = raw.trim().to_lowercase();
        let (number, scale) = raw.split_at(raw.len().saturating_sub(1));
        let value: u64 = number.parse().ok()?;
        match scale {
            "g" => Some(value << 30),
            "m" => Some(value << 20),
            "k" => Some(value << 10),
            "b" | "" => Some(value),
            _ => None,
        }
    }

    pub fn bind_mount_sources(&self) -> Vec<String> {
        self.volumes
            .as_ref()
            .map(|v| {
                v.iter()
                    .filter_map(|entry| entry.split(':').next().map(str::to_string))
                    .filter(|source| source.starts_with('.') || source.starts_with('/'))
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ComposeContract {
    pub services: BTreeMap<String, ComposeService>,
}

pub fn compose_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/r1/compose.yml")
}

pub fn load_compose() -> Result<ComposeContract> {
    let path = compose_path();
    let raw =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    Ok(serde_yaml::from_str(&raw).context("parsing compose.yml")?)
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn scorecard_fixture(
        cells: [CellVerdict; 5],
        p95_ratio: f64,
        churn_ratio: f64,
    ) -> Scorecard {
        let names = ["q1", "q2", "q3", "q4", "q5"];
        Scorecard {
            cells: names
                .iter()
                .zip(cells)
                .map(|(n, c)| (n.to_string(), c))
                .collect(),
            geometric_p95_ratio: p95_ratio,
            churn_ratio,
        }
    }

    #[test]
    fn winner_rule_requires_four_wins_no_losses_and_both_aggregate_bounds() {
        let scorecard = scorecard_fixture([CellVerdict::GrayDbWin; 5], 0.88, 0.75);
        assert!(scorecard.graydb_beat_clickhouse());

        let with_loss = scorecard_fixture(
            [
                CellVerdict::GrayDbWin,
                CellVerdict::GrayDbWin,
                CellVerdict::GrayDbWin,
                CellVerdict::GrayDbWin,
                CellVerdict::ClickHouseWin,
            ],
            0.88,
            0.75,
        );
        assert!(!with_loss.graydb_beat_clickhouse());
    }

    #[test]
    fn winner_rule_rejects_weak_p95_and_weak_churn_bounds() {
        let weak_p95 = scorecard_fixture([CellVerdict::GrayDbWin; 5], 0.95, 0.75);
        assert!(
            !weak_p95.graydb_beat_clickhouse(),
            "p95 ratio 0.95 must fail the 10% bound"
        );
        let weak_churn = scorecard_fixture([CellVerdict::GrayDbWin; 5], 0.88, 0.85);
        assert!(
            !weak_churn.graydb_beat_clickhouse(),
            "churn ratio 0.85 must fail the 20% bound"
        );
    }

    #[test]
    fn winner_rule_requires_1000_and_the_highest_common_sustainable_rate() {
        // This catches evaluating only the most favorable rate, which could
        // allow winner language while the required 1,000 rows/s stage loses.
        let strong = scorecard_fixture([CellVerdict::GrayDbWin; 5], 0.88, 0.75);
        let weak = scorecard_fixture([CellVerdict::ClickHouseWin; 5], 1.10, 1.25);
        let required = BTreeMap::from([(1_000, strong.clone()), (2_000, strong)]);
        assert!(Scorecard::evaluate(&required).graydb_beat_clickhouse());

        let misses_1000 = BTreeMap::from([
            (1_000, weak),
            (
                2_000,
                scorecard_fixture([CellVerdict::GrayDbWin; 5], 0.88, 0.75),
            ),
        ]);
        assert!(!Scorecard::evaluate(&misses_1000).graydb_beat_clickhouse());
    }

    #[test]
    fn winner_rule_requires_the_complete_q1_through_q5_suite() {
        let incomplete = Scorecard {
            cells: BTreeMap::from([
                ("q1".into(), CellVerdict::GrayDbWin),
                ("q2".into(), CellVerdict::GrayDbWin),
                ("q3".into(), CellVerdict::GrayDbWin),
                ("q4".into(), CellVerdict::GrayDbWin),
            ]),
            geometric_p95_ratio: 0.80,
            churn_ratio: 0.70,
        };
        assert!(!incomplete.graydb_beat_clickhouse());
    }

    #[test]
    fn cell_rules_match_the_spec_thresholds() {
        // GrayDB 5%+ lower p95 and no-higher p99: win.
        assert_eq!(evaluate_cell(940, 990, 1000, 1000), CellVerdict::GrayDbWin);
        // p95 better but p99 worse: conflicting tail, never a win.
        assert_eq!(
            evaluate_cell(940, 1050, 1000, 1000),
            CellVerdict::ConflictingTail
        );
        // Both within 5%: tie.
        assert_eq!(evaluate_cell(980, 990, 1000, 1000), CellVerdict::Tie);
        // ClickHouse clearly better.
        assert_eq!(
            evaluate_cell(1100, 1150, 1000, 1000),
            CellVerdict::ClickHouseWin
        );
        // Exact threshold is a win, but a 5% p99 difference is not a tie.
        assert_eq!(evaluate_cell(950, 1000, 1000, 1000), CellVerdict::GrayDbWin);
        assert_eq!(
            evaluate_cell(950, 1050, 1000, 1000),
            CellVerdict::ConflictingTail
        );
    }

    #[test]
    fn compose_memory_limits_normalize_to_bytes() {
        let service = ComposeService {
            healthcheck: None,
            memory_limit: Some("3g".into()),
            volumes: None,
        };
        assert_eq!(service.memory_limit_bytes(), Some(3_u64 << 30));
        let mib = ComposeService {
            healthcheck: None,
            memory_limit: Some("4096m".into()),
            volumes: None,
        };
        assert_eq!(mib.memory_limit_bytes(), Some(4_u64 << 30));
    }

    #[test]
    fn compose_bind_sources_exclude_named_volumes() {
        let service = ComposeService {
            healthcheck: None,
            memory_limit: None,
            volumes: Some(vec![
                "./data:/var/lib/data".into(),
                "/absolute/logs:/logs:ro".into(),
                "named-volume:/var/lib/engine".into(),
            ]),
        };
        assert_eq!(
            service.bind_mount_sources(),
            vec!["./data".to_string(), "/absolute/logs".to_string()]
        );
    }
}
