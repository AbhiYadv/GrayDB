//! Dual correctness oracle: replays the committed ledger into in-memory logical
//! state and evaluates Q1-Q5 with exact integer arithmetic. Any gap, duplicate,
//! stale version, or ignored tombstone must fail `compare`.

use crate::query::{canonical_digest, render_sql, QueryId, QueryParameters};
use crate::verdict::RunInvalidation;
use crate::workload::{Operation, TransactionPlan};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::future::Future;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowDifference {
    pub table: String,
    pub primary_key: u64,
    pub expected_version: u64,
    pub actual_version: u64,
    pub target_checkpoint: u64,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrectnessVerdict {
    pub passed: bool,
    pub differences: Vec<RowDifference>,
    pub invalidations: Vec<RunInvalidation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TenantState {
    region: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CustomerState {
    tenant_id: u64,
    segment: String,
    email_domain: String,
    profile_json: String,
    created_at_micros: i64,
    version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderState {
    tenant_id: u64,
    customer_id: u64,
    status: String,
    channel: String,
    amount_cents: i64,
    created_at_micros: i64,
    updated_at_micros: i64,
    attributes_json: String,
    version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventState {
    order_id: u64,
    tenant_id: u64,
    event_type: String,
    event_at_micros: i64,
    metadata_json: String,
    version: u64,
}

/// A canonical, deterministic primary-key sample captured alongside Q1-Q5.
/// `values` contain source-column text exactly as it is fed to the canonical
/// digest encoder; `deleted` records a tombstone instead of silently omitting
/// it from an historic checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowSample {
    pub table: String,
    pub primary_key: u64,
    pub version: u64,
    pub deleted: bool,
    pub values: BTreeMap<String, String>,
}

/// In-memory reconstruction of logical row state from committed transactions.
/// Tenants are immutable dimensions loaded at the initial snapshot (version 0);
/// customers, orders, and events are keyed state mutated only by `apply`.
#[derive(Debug, Clone, Default)]
pub struct LedgerOracle {
    tenants: BTreeMap<u64, TenantState>,
    customers: BTreeMap<u64, CustomerState>,
    orders: BTreeMap<u64, OrderState>,
    events: BTreeMap<u64, EventState>,
    order_history: BTreeMap<u64, Vec<(u64, Option<OrderState>)>>,
    event_history: BTreeMap<u64, Vec<(u64, Option<EventState>)>>,
    customer_history: BTreeMap<u64, Vec<(u64, Option<CustomerState>)>>,
    /// sequence -> operation hash for every applied plan (duplicate detection).
    applied: BTreeMap<u64, String>,
    /// How many times each sequence was applied; anything above one is a
    /// duplicate change even when the row state happens to be idempotent.
    applied_counts: BTreeMap<u64, u32>,
    /// commit-end LSN at which each sequence became visible.
    sequence_lsn: BTreeMap<u64, u64>,
}

pub const SEQUENCE_TABLE: &str = "_sequence";
const DAY_MICROS: i64 = 86_400_000_000;

impl LedgerOracle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load_tenant(&mut self, tenant_id: u64, region: impl Into<String>) {
        self.tenants.insert(
            tenant_id,
            TenantState {
                region: region.into(),
            },
        );
    }

    pub fn latest_checkpoint(&self) -> u64 {
        self.sequence_lsn.values().copied().max().unwrap_or(0)
    }

    pub fn applied_sequences(&self) -> impl Iterator<Item = (u64, &'_ str)> + '_ {
        self.applied.iter().map(|(s, h)| (*s, h.as_str()))
    }

    /// Applies one committed plan at its commit-end LSN. Requires a contiguous
    /// sequence and a hash match with the plan's own recorded hash chain.
    pub fn apply(&mut self, plan: &TransactionPlan, commit_lsn: u64) -> Result<()> {
        ensure!(
            operation_hash(&plan.operations) == plan.operation_sha256,
            "sequence {} operation hash does not match its operations",
            plan.sequence
        );
        if let Some(existing) = self.applied.get(&plan.sequence) {
            ensure!(
                existing == &plan.operation_sha256,
                "sequence {} re-applied with a different hash",
                plan.sequence
            );
            anyhow::bail!("sequence {} applied more than once", plan.sequence);
        }
        let expected = self.applied.keys().last().map(|s| s + 1).unwrap_or(1);
        ensure!(
            plan.sequence == expected,
            "ledger oracle sequence {} gap: expected {}",
            plan.sequence,
            expected
        );
        if let Some(previous_lsn) = self.sequence_lsn.values().last().copied() {
            ensure!(
                commit_lsn > previous_lsn,
                "sequence {} commit LSN {} is not greater than previous LSN {}",
                plan.sequence,
                commit_lsn,
                previous_lsn
            );
        }
        self.apply_unchecked(plan, commit_lsn)
    }

    /// Applies without contiguity validation. Used by fixtures that inject
    /// deliberate corruption; production code never calls this.
    pub fn apply_unchecked(&mut self, plan: &TransactionPlan, commit_lsn: u64) -> Result<()> {
        if let Some(existing) = self.applied.get(&plan.sequence) {
            ensure!(
                existing == &plan.operation_sha256,
                "sequence {} re-applied with a different hash",
                plan.sequence
            );
        }
        *self.applied_counts.entry(plan.sequence).or_insert(0) += 1;
        for operation in &plan.operations {
            self.apply_operation(operation, commit_lsn);
        }
        self.applied
            .insert(plan.sequence, plan.operation_sha256.clone());
        self.sequence_lsn.insert(plan.sequence, commit_lsn);
        Ok(())
    }

    fn apply_operation(&mut self, operation: &Operation, version: u64) {
        match operation {
            Operation::InsertCustomer(row) => {
                self.customers.insert(
                    row.customer_id,
                    CustomerState {
                        tenant_id: row.tenant_id,
                        segment: row.segment.clone(),
                        email_domain: row.email_domain.clone(),
                        profile_json: row.profile_json.clone(),
                        created_at_micros: row.created_at_micros,
                        version,
                    },
                );
                let state = self.customers[&row.customer_id].clone();
                self.customer_history
                    .entry(row.customer_id)
                    .or_default()
                    .push((version, Some(state)));
            }
            Operation::InsertOrder(row) => {
                let state = OrderState {
                    tenant_id: row.tenant_id,
                    customer_id: row.customer_id,
                    status: row.status.clone(),
                    channel: row.channel.clone(),
                    amount_cents: row.amount_cents,
                    created_at_micros: row.created_at_micros,
                    updated_at_micros: row.updated_at_micros,
                    attributes_json: row.attributes_json.clone(),
                    version,
                };
                self.orders.insert(row.order_id, state.clone());
                self.order_history
                    .entry(row.order_id)
                    .or_default()
                    .push((version, Some(state)));
            }
            Operation::InsertOrderEvent(row) => {
                let state = EventState {
                    order_id: row.order_id,
                    tenant_id: row.tenant_id,
                    event_type: row.event_type.clone(),
                    event_at_micros: row.event_at_micros,
                    metadata_json: row.metadata_json.clone(),
                    version,
                };
                self.events.insert(row.event_id, state.clone());
                self.event_history
                    .entry(row.event_id)
                    .or_default()
                    .push((version, Some(state)));
            }
            Operation::UpdateCustomer {
                customer_id,
                tenant_id,
                segment,
                email_domain,
                profile_json,
                created_at_micros,
                ..
            } => {
                if let Some(state) = self.customers.get_mut(customer_id) {
                    state.tenant_id = *tenant_id;
                    state.segment = segment.clone();
                    state.email_domain = email_domain.clone();
                    state.profile_json = profile_json.clone();
                    state.created_at_micros = *created_at_micros;
                    state.version = version;
                    self.customer_history
                        .entry(*customer_id)
                        .or_default()
                        .push((version, Some(state.clone())));
                }
            }
            Operation::UpdateOrder {
                order_id,
                tenant_id,
                customer_id,
                status,
                channel,
                amount_cents,
                created_at_micros,
                updated_at_micros,
                attributes_json,
                ..
            } => {
                if let Some(state) = self.orders.get_mut(order_id) {
                    state.tenant_id = *tenant_id;
                    state.customer_id = *customer_id;
                    state.status = status.clone();
                    state.channel = channel.clone();
                    state.amount_cents = *amount_cents;
                    state.created_at_micros = *created_at_micros;
                    state.updated_at_micros = *updated_at_micros;
                    state.attributes_json = attributes_json.clone();
                    state.version = version;
                    self.order_history
                        .entry(*order_id)
                        .or_default()
                        .push((version, Some(state.clone())));
                }
            }
            Operation::DeleteOrder { order_id, .. } => {
                self.orders.remove(order_id);
                self.order_history
                    .entry(*order_id)
                    .or_default()
                    .push((version, None));
            }
            Operation::DeleteOrderEvent { event_id, .. } => {
                self.events.remove(event_id);
                self.event_history
                    .entry(*event_id)
                    .or_default()
                    .push((version, None));
            }
        }
    }

    fn orders_at(&self, target_lsn: u64) -> BTreeMap<u64, OrderState> {
        self.order_history
            .iter()
            .filter_map(|(key, versions)| {
                versions
                    .iter()
                    .rev()
                    .find(|(version, _)| *version <= target_lsn)
                    .and_then(|(_, state)| state.clone().map(|state| (*key, state)))
            })
            .collect()
    }

    fn customers_at(&self, target_lsn: u64) -> BTreeMap<u64, CustomerState> {
        self.customer_history
            .iter()
            .filter_map(|(key, versions)| {
                versions
                    .iter()
                    .rev()
                    .find(|(version, _)| *version <= target_lsn)
                    .and_then(|(_, state)| state.clone().map(|state| (*key, state)))
            })
            .collect()
    }

    fn events_at(&self, target_lsn: u64) -> BTreeMap<u64, EventState> {
        self.event_history
            .iter()
            .filter_map(|(key, versions)| {
                versions
                    .iter()
                    .rev()
                    .find(|(version, _)| *version <= target_lsn)
                    .and_then(|(_, state)| state.clone().map(|state| (*key, state)))
            })
            .collect()
    }

    /// Returns deterministic primary-key samples for all source tables at a
    /// checkpoint. BTreeMap order makes the sample independent of wall-clock
    /// execution, process scheduling, and hash-map iteration order.
    pub fn sample_rows_at(&self, target_lsn: u64, per_table_limit: usize) -> Vec<RowSample> {
        let mut samples = Vec::new();
        let limit = per_table_limit.max(1);

        for (id, tenant) in self.tenants.iter().take(limit) {
            samples.push(RowSample {
                table: "r1.tenants".into(),
                primary_key: *id,
                version: 0,
                deleted: false,
                values: BTreeMap::from([
                    ("tenant_id".into(), id.to_string()),
                    ("region".into(), tenant.region.clone()),
                ]),
            });
        }
        for (id, versions) in self.customer_history.iter().take(limit) {
            let Some((version, Some(customer))) = versions
                .iter()
                .rev()
                .find(|(version, _)| *version <= target_lsn)
            else {
                continue;
            };
            samples.push(RowSample {
                table: "r1.customers".into(),
                primary_key: *id,
                version: *version,
                deleted: false,
                values: BTreeMap::from([
                    ("customer_id".into(), id.to_string()),
                    ("tenant_id".into(), customer.tenant_id.to_string()),
                    ("segment".into(), customer.segment.clone()),
                    ("email_domain".into(), customer.email_domain.clone()),
                    ("profile".into(), customer.profile_json.clone()),
                    (
                        "created_at_micros".into(),
                        customer.created_at_micros.to_string(),
                    ),
                ]),
            });
        }
        for (id, versions) in self.order_history.iter().take(limit) {
            let Some((version, state)) = versions
                .iter()
                .rev()
                .find(|(version, _)| *version <= target_lsn)
            else {
                continue;
            };
            let (deleted, values) = match state {
                Some(order) => (
                    false,
                    BTreeMap::from([
                        ("order_id".into(), id.to_string()),
                        ("tenant_id".into(), order.tenant_id.to_string()),
                        ("customer_id".into(), order.customer_id.to_string()),
                        ("status".into(), order.status.clone()),
                        ("channel".into(), order.channel.clone()),
                        ("amount_cents".into(), order.amount_cents.to_string()),
                        (
                            "created_at_micros".into(),
                            order.created_at_micros.to_string(),
                        ),
                        (
                            "updated_at_micros".into(),
                            order.updated_at_micros.to_string(),
                        ),
                        ("attributes".into(), order.attributes_json.clone()),
                    ]),
                ),
                None => (true, BTreeMap::from([("order_id".into(), id.to_string())])),
            };
            samples.push(RowSample {
                table: "r1.orders".into(),
                primary_key: *id,
                version: *version,
                deleted,
                values,
            });
        }
        for (id, versions) in self.event_history.iter().take(limit) {
            let Some((version, state)) = versions
                .iter()
                .rev()
                .find(|(version, _)| *version <= target_lsn)
            else {
                continue;
            };
            let (deleted, values) = match state {
                Some(event) => (
                    false,
                    BTreeMap::from([
                        ("event_id".into(), id.to_string()),
                        ("order_id".into(), event.order_id.to_string()),
                        ("tenant_id".into(), event.tenant_id.to_string()),
                        ("event_type".into(), event.event_type.clone()),
                        ("event_at_micros".into(), event.event_at_micros.to_string()),
                        ("metadata".into(), event.metadata_json.clone()),
                    ]),
                ),
                None => (true, BTreeMap::from([("event_id".into(), id.to_string())])),
            };
            samples.push(RowSample {
                table: "r1.order_events".into(),
                primary_key: *id,
                version: *version,
                deleted,
                values,
            });
        }
        samples
    }

    /// Evaluates one query of Q1-Q5 at a target LSN with exact integer math.
    pub fn query_at(
        &self,
        id: QueryId,
        params: &QueryParameters,
        target_lsn: u64,
    ) -> crate::query::QueryResult {
        let orders = self.orders_at(target_lsn);
        let events = self.events_at(target_lsn);
        let visible_orders = || orders.iter();
        match id {
            QueryId::Q1 => {
                let window_start = params.window_end_micros - 7 * DAY_MICROS;
                let mut groups: BTreeMap<u64, (i64, u64)> = BTreeMap::new();
                for (_, o) in visible_orders().filter(|(_, o)| o.created_at_micros >= window_start)
                {
                    let entry = groups.entry(o.customer_id).or_insert((0, 0));
                    entry.0 += o.amount_cents;
                    entry.1 += 1;
                }
                crate::query::QueryResult {
                    columns: vec![
                        "customer_id".into(),
                        "sum(amount_cents)".into(),
                        "count(*)".into(),
                    ],
                    rows: groups
                        .into_iter()
                        .map(|(customer, (sum, count))| {
                            vec![
                                Some(customer.to_string()),
                                Some(sum.to_string()),
                                Some(count.to_string()),
                            ]
                        })
                        .collect(),
                }
            }
            QueryId::Q2 => {
                let mut groups: BTreeMap<&str, u64> = BTreeMap::new();
                for (_, o) in visible_orders().filter(|(_, o)| o.tenant_id == params.tenant_id) {
                    *groups.entry(o.status.as_str()).or_insert(0) += 1;
                }
                crate::query::QueryResult {
                    columns: vec!["status".into(), "count(*)".into()],
                    rows: groups
                        .into_iter()
                        .map(|(status, count)| {
                            vec![Some(status.to_string()), Some(count.to_string())]
                        })
                        .collect(),
                }
            }
            QueryId::Q3 => {
                let mut groups: BTreeMap<(&str, &str, &str), (i64, u64)> = BTreeMap::new();
                for (_, o) in visible_orders() {
                    let Some(tenant) = self.tenants.get(&o.tenant_id) else {
                        continue;
                    };
                    let key = (
                        tenant.region.as_str(),
                        o.channel.as_str(),
                        o.status.as_str(),
                    );
                    let entry = groups.entry(key).or_insert((0, 0));
                    entry.0 += o.amount_cents;
                    entry.1 += 1;
                }
                crate::query::QueryResult {
                    columns: vec![
                        "region".into(),
                        "channel".into(),
                        "status".into(),
                        "sum(amount_cents)".into(),
                        "count(*)".into(),
                    ],
                    rows: groups
                        .into_iter()
                        .map(|((region, channel, status), (sum, count))| {
                            vec![
                                Some(region.to_string()),
                                Some(channel.to_string()),
                                Some(status.to_string()),
                                Some(sum.to_string()),
                                Some(count.to_string()),
                            ]
                        })
                        .collect(),
                }
            }
            QueryId::Q4 => {
                let window_start = params.window_end_micros - DAY_MICROS;
                let mut groups: BTreeMap<&str, u64> = BTreeMap::new();
                for (_, e) in events
                    .iter()
                    .filter(|(_, e)| params.tenant_set.contains(&e.tenant_id))
                    .filter(|(_, e)| e.event_at_micros >= window_start)
                {
                    *groups.entry(e.event_type.as_str()).or_insert(0) += 1;
                }
                crate::query::QueryResult {
                    columns: vec!["event_type".into(), "count(*)".into()],
                    rows: groups
                        .into_iter()
                        .map(|(event_type, count)| {
                            vec![Some(event_type.to_string()), Some(count.to_string())]
                        })
                        .collect(),
                }
            }
            QueryId::Q5 => {
                let mut groups: BTreeMap<&str, (u64, i64)> = BTreeMap::new();
                for (_, o) in visible_orders() {
                    let entry = groups.entry(o.status.as_str()).or_insert((0, 0));
                    entry.0 += 1;
                    entry.1 += o.amount_cents;
                }
                crate::query::QueryResult {
                    columns: vec![
                        "status".into(),
                        "count(*)".into(),
                        "sum(amount_cents)".into(),
                    ],
                    rows: groups
                        .into_iter()
                        .map(|(status, (count, sum))| {
                            vec![
                                Some(status.to_string()),
                                Some(count.to_string()),
                                Some(sum.to_string()),
                            ]
                        })
                        .collect(),
                }
            }
        }
    }

    /// Public Task 9 interface alias for exact checkpoint queries.
    pub fn query(
        &self,
        id: QueryId,
        params: &QueryParameters,
        target_lsn: u64,
    ) -> crate::query::QueryResult {
        self.query_at(id, params, target_lsn)
    }

    /// Compares this expected state against a candidate at the latest recorded
    /// checkpoint. Sequence gaps and duplicates are differences, not errors.
    pub fn compare(&self, candidate: &LedgerOracle) -> CorrectnessVerdict {
        self.compare_at(candidate, self.latest_checkpoint())
    }

    /// Compares the complete state visible at one source LSN. This deliberately
    /// reconstructs each table from history instead of filtering the current
    /// map by version: a later update or tombstone must not erase the exact
    /// historic state used by an earlier checkpoint.
    pub fn compare_at(&self, candidate: &LedgerOracle, target: u64) -> CorrectnessVerdict {
        let mut differences = Vec::new();

        for (sequence, expected_hash) in self
            .applied
            .iter()
            .filter(|(sequence, _)| self.sequence_lsn.get(sequence).copied().unwrap_or(0) <= target)
        {
            match candidate.applied.get(sequence) {
                None => differences.push(RowDifference {
                    table: SEQUENCE_TABLE.into(),
                    primary_key: *sequence,
                    expected_version: *sequence as u64,
                    actual_version: 0,
                    target_checkpoint: target,
                    detail: format!("missing committed sequence {sequence}"),
                }),
                Some(actual_hash) if actual_hash != expected_hash => {
                    differences.push(RowDifference {
                        table: SEQUENCE_TABLE.into(),
                        primary_key: *sequence,
                        expected_version: *sequence as u64,
                        actual_version: *sequence as u64,
                        target_checkpoint: target,
                        detail: format!("sequence {sequence} hash mismatch"),
                    });
                }
                _ => {
                    let expected_lsn = self.sequence_lsn.get(sequence).copied().unwrap_or(0);
                    let actual_lsn = candidate.sequence_lsn.get(sequence).copied().unwrap_or(0);
                    if expected_lsn != actual_lsn {
                        let after = self
                            .sequence_lsn
                            .iter()
                            .find_map(|(other, lsn)| (*lsn == actual_lsn).then_some(*other))
                            .unwrap_or(*sequence);
                        differences.push(RowDifference { table: SEQUENCE_TABLE.into(), primary_key: *sequence, expected_version: *sequence, actual_version: after, target_checkpoint: target, detail: format!("state-changing reorder: sequence {sequence} expected LSN {expected_lsn}, candidate LSN {actual_lsn}") });
                    }
                }
            }
        }
        for sequence in candidate
            .applied
            .keys()
            .filter(|sequence| candidate.sequence_lsn.get(sequence).copied().unwrap_or(0) <= target)
        {
            if !self.applied.contains_key(sequence)
                || self.sequence_lsn.get(sequence).copied().unwrap_or(0) > target
            {
                differences.push(RowDifference {
                    table: SEQUENCE_TABLE.into(),
                    primary_key: *sequence,
                    expected_version: 0,
                    actual_version: *sequence as u64,
                    target_checkpoint: target,
                    detail: format!("duplicate or unknown sequence {sequence}"),
                });
            } else if *candidate.applied_counts.get(sequence).unwrap_or(&0) > 1 {
                differences.push(RowDifference {
                    table: SEQUENCE_TABLE.into(),
                    primary_key: *sequence,
                    expected_version: *sequence as u64,
                    actual_version: *sequence as u64,
                    target_checkpoint: target,
                    detail: format!("sequence {sequence} applied multiple times"),
                });
            }
        }

        self.compare_tenants(candidate, target, &mut differences);
        self.compare_orders(candidate, target, &mut differences);
        self.compare_customers(candidate, target, &mut differences);
        self.compare_events(candidate, target, &mut differences);

        let invalidations = invalidations_for(&differences);
        CorrectnessVerdict {
            passed: differences.is_empty(),
            differences,
            invalidations,
        }
    }

    fn compare_orders(&self, candidate: &LedgerOracle, target: u64, out: &mut Vec<RowDifference>) {
        let expected_rows = self.orders_at(target);
        let actual_rows = candidate.orders_at(target);
        let keys: std::collections::BTreeSet<_> =
            expected_rows.keys().chain(actual_rows.keys()).collect();
        for key in keys {
            let expected = expected_rows.get(key);
            let actual = actual_rows.get(key);
            match (expected, actual) {
                (None, None) => {}
                (Some(e), Some(a)) => {
                    if e != a {
                        out.push(RowDifference {
                            table: "r1.orders".into(),
                            primary_key: *key,
                            expected_version: e.version,
                            actual_version: a.version,
                            target_checkpoint: target,
                            detail: format!(
                                "order {key} state mismatch: expected {:?}, actual {:?}",
                                (e.status.as_str(), e.channel.as_str(), e.amount_cents),
                                (a.status.as_str(), a.channel.as_str(), a.amount_cents)
                            ),
                        });
                    }
                }
                (Some(e), None) => out.push(RowDifference {
                    table: "r1.orders".into(),
                    primary_key: *key,
                    expected_version: e.version,
                    actual_version: 0,
                    target_checkpoint: target,
                    detail: format!("order {key} missing in candidate (tombstone ignored or lost)"),
                }),
                (None, Some(a)) => out.push(RowDifference {
                    table: "r1.orders".into(),
                    primary_key: *key,
                    expected_version: 0,
                    actual_version: a.version,
                    target_checkpoint: target,
                    detail: format!("order {key} present in candidate but deleted at checkpoint"),
                }),
            }
        }
    }

    fn compare_tenants(&self, candidate: &LedgerOracle, target: u64, out: &mut Vec<RowDifference>) {
        let keys: std::collections::BTreeSet<_> = self
            .tenants
            .keys()
            .chain(candidate.tenants.keys())
            .collect();
        for key in keys {
            let expected = self.tenants.get(key);
            let actual = candidate.tenants.get(key);
            if expected != actual {
                out.push(RowDifference {
                    table: "r1.tenants".into(),
                    primary_key: *key,
                    expected_version: 0,
                    actual_version: 0,
                    target_checkpoint: target,
                    detail: format!("immutable tenant {key} state mismatch"),
                });
            }
        }
    }

    fn compare_customers(
        &self,
        candidate: &LedgerOracle,
        target: u64,
        out: &mut Vec<RowDifference>,
    ) {
        let expected_rows = self.customers_at(target);
        let actual_rows = candidate.customers_at(target);
        let keys: std::collections::BTreeSet<_> =
            expected_rows.keys().chain(actual_rows.keys()).collect();
        for key in keys {
            let expected = expected_rows.get(key);
            let actual = actual_rows.get(key);
            if expected != actual {
                out.push(RowDifference {
                    table: "r1.customers".into(),
                    primary_key: *key,
                    expected_version: expected.map(|c| c.version).unwrap_or(0),
                    actual_version: actual.map(|c| c.version).unwrap_or(0),
                    target_checkpoint: target,
                    detail: format!("customer {key} state mismatch"),
                });
            }
        }
    }

    fn compare_events(&self, candidate: &LedgerOracle, target: u64, out: &mut Vec<RowDifference>) {
        let expected_rows = self.events_at(target);
        let actual_rows = candidate.events_at(target);
        let keys: std::collections::BTreeSet<_> =
            expected_rows.keys().chain(actual_rows.keys()).collect();
        for key in keys {
            let expected = expected_rows.get(key);
            let actual = actual_rows.get(key);
            if expected != actual {
                out.push(RowDifference {
                    table: "r1.order_events".into(),
                    primary_key: *key,
                    expected_version: expected.map(|e| e.version).unwrap_or(0),
                    actual_version: actual.map(|e| e.version).unwrap_or(0),
                    target_checkpoint: target,
                    detail: format!("order_event {key} state mismatch"),
                });
            }
        }
    }
}

fn operation_hash(operations: &[Operation]) -> String {
    let bytes = serde_json::to_vec(operations).expect("operation serialization must not fail");
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Pauses and drains the canonical writer around a checkpoint so the captured
/// LSN covers a quiesced, fully-committed prefix. The controller (Task 12)
/// implements this over its writer handle.
#[async_trait::async_trait]
pub trait WriterControl: Send + Sync {
    async fn pause(&self) -> Result<()>;
    /// Returns only when no transaction is in flight.
    async fn drain(&self) -> Result<()>;
    async fn resume(&self) -> Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapturedCheckpoint {
    pub source_lsn: u64,
    pub query_digests: BTreeMap<String, String>,
    pub samples: Vec<RowSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineCheckpointEvidence {
    pub engine: String,
    pub query_digests: BTreeMap<String, String>,
    pub samples: Vec<RowSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifiedCheckpoint {
    pub checkpoint: CapturedCheckpoint,
    pub engines: Vec<EngineCheckpointEvidence>,
    pub verdict: CorrectnessVerdict,
}

/// An engine endpoint capable of supplying both the scored Q1-Q5 result and
/// deterministic row samples at one exact source LSN. This is intentionally a
/// separate contract from `EngineAdapter`: samples are correctness evidence and
/// must never be folded into measured query timings.
#[async_trait::async_trait]
pub trait SampledCheckpointEngine: Send + Sync {
    fn name(&self) -> &str;
    async fn wait_visible(&self, target_lsn: u64, timeout: std::time::Duration) -> Result<()>;
    async fn query(
        &self,
        invocation: &crate::adapter::QueryInvocation,
    ) -> Result<crate::adapter::QueryResult>;
    async fn samples(&self, target_lsn: u64, requested: &[RowSample]) -> Result<Vec<RowSample>>;
}

/// Implementations must atomically flush the checkpoint record before
/// returning. Keeping this contract explicit prevents a resumed writer from
/// racing ahead of an undurable correctness decision.
#[async_trait::async_trait]
pub trait CheckpointVerdictSink: Send + Sync {
    async fn persist(&self, record: &VerifiedCheckpoint) -> Result<()>;
}

/// PostgreSQL checkpoint oracle: captures a repeatable-read snapshot at a
/// quiesced source LSN and digests Q1-Q5 exactly as the engines are digested.
pub struct PostgresCheckpoint;

pub const QUERIES: [(&str, QueryId); 5] = [
    ("q1", QueryId::Q1),
    ("q2", QueryId::Q2),
    ("q3", QueryId::Q3),
    ("q4", QueryId::Q4),
    ("q5", QueryId::Q5),
];

impl PostgresCheckpoint {
    /// Runs a checkpoint body while the canonical writer is paused and drained.
    /// Once `pause` succeeds, `resume` is attempted exactly once on *every*
    /// subsequent path, including drain, source-query, comparison, and durable
    /// record failures.
    pub async fn with_paused_writer<T>(
        writer: &dyn WriterControl,
        capture: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        writer.pause().await?;
        let capture_result = async {
            writer.drain().await?;
            capture.await
        }
        .await;
        let resume_result = writer.resume().await;
        match (capture_result, resume_result) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(capture_error), Ok(())) => Err(capture_error),
            (Ok(_), Err(resume_error)) => Err(resume_error),
            (Err(capture_error), Err(resume_error)) => Err(anyhow::anyhow!(
                "checkpoint failed: {capture_error}; writer resume also failed: {resume_error}"
            )),
        }
    }

    /// Captures PostgreSQL-only evidence. The benchmark controller must use
    /// [`Self::capture`] for scored correctness checkpoints so engine barriers,
    /// comparisons, durability, and safe writer resume are not bypassed.
    pub async fn capture_source(
        client: &mut tokio_postgres::Client,
        writer: &dyn WriterControl,
        params: &QueryParameters,
    ) -> Result<CapturedCheckpoint> {
        Self::with_paused_writer(writer, Self::capture_quiesced(client, params, None)).await
    }

    /// Full checkpoint protocol. It holds the writer pause through PostgreSQL
    /// repeatable-read capture, both engine barriers, canonical digest and
    /// row-sample comparison, and the durable verdict write. No checkpoint work
    /// is part of a measured query window.
    pub async fn capture(
        client: &mut tokio_postgres::Client,
        writer: &dyn WriterControl,
        oracle: &LedgerOracle,
        checkpoint: crate::contracts::LogicalCheckpoint,
        params: &QueryParameters,
        engines: &[&dyn SampledCheckpointEngine],
        engine_timeout: std::time::Duration,
        sink: &dyn CheckpointVerdictSink,
    ) -> Result<VerifiedCheckpoint> {
        Self::with_paused_writer(writer, async {
            let captured = Self::capture_quiesced(client, params, Some(oracle)).await?;
            let expected_samples = oracle.sample_rows_at(captured.source_lsn, 16);
            let logical_checkpoint = crate::contracts::LogicalCheckpoint {
                sequence: checkpoint.sequence,
                source_lsn: captured.source_lsn,
            };
            let mut differences = Vec::new();
            let ledger_digests = ledger_query_digests(oracle, params, captured.source_lsn);
            for (name, id) in QUERIES {
                if captured.query_digests.get(name) != ledger_digests.get(name) {
                    differences.push(RowDifference {
                        table: "postgres".into(),
                        primary_key: id as u64,
                        expected_version: captured.source_lsn,
                        actual_version: captured.source_lsn,
                        target_checkpoint: captured.source_lsn,
                        detail: format!("{name} PostgreSQL digest disagrees with ledger oracle"),
                    });
                }
            }
            compare_samples(
                &expected_samples,
                &captured.samples,
                captured.source_lsn,
                "postgres",
                &mut differences,
            );
            let mut evidence = Vec::with_capacity(engines.len());
            for engine in engines {
                engine
                    .wait_visible(captured.source_lsn, engine_timeout)
                    .await?;
                let mut query_digests = BTreeMap::new();
                for (name, id) in QUERIES {
                    let result = engine
                        .query(&crate::adapter::QueryInvocation {
                            id,
                            parameters: params.clone(),
                            checkpoint: logical_checkpoint,
                            target_lsn: captured.source_lsn,
                        })
                        .await?;
                    if result.target_lsn != captured.source_lsn
                        || result.visible_lsn < captured.source_lsn
                    {
                        differences.push(RowDifference {
                            table: format!("engine:{}", engine.name()),
                            primary_key: id as u64,
                            expected_version: captured.source_lsn,
                            actual_version: result.visible_lsn,
                            target_checkpoint: captured.source_lsn,
                            detail: format!("{name} returned stale or mismatched LSN proof"),
                        });
                    }
                    let digest = canonical_digest(&crate::query::QueryResult {
                        columns: result.columns,
                        rows: result.rows,
                    });
                    if captured.query_digests.get(name) != Some(&digest) {
                        differences.push(RowDifference {
                            table: format!("engine:{}", engine.name()),
                            primary_key: id as u64,
                            expected_version: captured.source_lsn,
                            actual_version: result.visible_lsn,
                            target_checkpoint: captured.source_lsn,
                            detail: format!("{name} canonical digest mismatch"),
                        });
                    }
                    if ledger_digests.get(name) != Some(&digest) {
                        differences.push(RowDifference {
                            table: format!("engine:{}", engine.name()),
                            primary_key: id as u64,
                            expected_version: captured.source_lsn,
                            actual_version: result.visible_lsn,
                            target_checkpoint: captured.source_lsn,
                            detail: format!("{name} digest disagrees with ledger oracle"),
                        });
                    }
                    query_digests.insert(name.to_string(), digest);
                }
                let samples = engine
                    .samples(captured.source_lsn, &captured.samples)
                    .await?;
                compare_samples(
                    &captured.samples,
                    &samples,
                    captured.source_lsn,
                    engine.name(),
                    &mut differences,
                );
                evidence.push(EngineCheckpointEvidence {
                    engine: engine.name().to_string(),
                    query_digests,
                    samples,
                });
            }
            let record = VerifiedCheckpoint {
                checkpoint: captured,
                engines: evidence,
                verdict: CorrectnessVerdict {
                    passed: differences.is_empty(),
                    invalidations: invalidations_for(&differences),
                    differences,
                },
            };
            sink.persist(&record).await?;
            Ok(record)
        })
        .await
    }

    /// Explicit alias for callers that want the verification intent in the
    /// method name. It has exactly the same all-or-nothing checkpoint contract
    /// as [`Self::capture`].
    pub async fn capture_and_verify(
        client: &mut tokio_postgres::Client,
        writer: &dyn WriterControl,
        oracle: &LedgerOracle,
        checkpoint: crate::contracts::LogicalCheckpoint,
        params: &QueryParameters,
        engines: &[&dyn SampledCheckpointEngine],
        engine_timeout: std::time::Duration,
        sink: &dyn CheckpointVerdictSink,
    ) -> Result<VerifiedCheckpoint> {
        Self::capture(
            client,
            writer,
            oracle,
            checkpoint,
            params,
            engines,
            engine_timeout,
            sink,
        )
        .await
    }

    async fn capture_quiesced(
        client: &mut tokio_postgres::Client,
        params: &QueryParameters,
        oracle: Option<&LedgerOracle>,
    ) -> Result<CapturedCheckpoint> {
        let transaction = client
            .build_transaction()
            .read_only(true)
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .start()
            .await?;
        let lsn: String = transaction
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await?
            .get(0);
        let mut query_digests = BTreeMap::new();
        for (name, id) in QUERIES {
            let sql_file = match id {
                QueryId::Q1 => include_str!("../../../bench/r1/queries/q1.sql"),
                QueryId::Q2 => include_str!("../../../bench/r1/queries/q2.sql"),
                QueryId::Q3 => include_str!("../../../bench/r1/queries/q3.sql"),
                QueryId::Q4 => include_str!("../../../bench/r1/queries/q4.sql"),
                QueryId::Q5 => include_str!("../../../bench/r1/queries/q5.sql"),
            };
            let sql = render_sql(sql_file, params).map_err(anyhow::Error::msg)?;
            let rows = transaction.query(&sql, &[]).await?;
            let columns: Vec<String> = rows
                .first()
                .map(|row| row.columns().iter().map(|c| c.name().to_string()).collect())
                .unwrap_or_default();
            let rendered: Vec<Vec<Option<String>>> = rows
                .iter()
                .map(|row| {
                    row.columns()
                        .iter()
                        .enumerate()
                        .map(|(i, _)| postgres_cell_text(row, i))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            query_digests.insert(
                name.to_string(),
                canonical_digest(&crate::query::QueryResult {
                    columns,
                    rows: rendered,
                }),
            );
        }
        let parts: Vec<&str> = lsn.split('/').collect();
        ensure!(parts.len() == 2, "invalid pg_current_wal_lsn: {lsn}");
        let source_lsn = (u32::from_str_radix(parts[0], 16)? as u64) << 32
            | u32::from_str_radix(parts[1], 16)? as u64;
        let requested_samples = oracle
            .map(|oracle| oracle.sample_rows_at(source_lsn, 16))
            .unwrap_or_default();
        let samples = capture_postgres_samples(&transaction, &requested_samples).await?;
        transaction.commit().await?;
        Ok(CapturedCheckpoint {
            source_lsn,
            query_digests,
            samples,
        })
    }
}

fn ledger_query_digests(
    oracle: &LedgerOracle,
    params: &QueryParameters,
    target_lsn: u64,
) -> BTreeMap<String, String> {
    QUERIES
        .into_iter()
        .map(|(name, id)| {
            (
                name.to_string(),
                canonical_digest(&oracle.query(id, params, target_lsn)),
            )
        })
        .collect()
}

fn invalidations_for(differences: &[RowDifference]) -> Vec<RunInvalidation> {
    let mut out = Vec::new();
    for difference in differences {
        let detail = difference.detail.as_str();
        let reason = if detail.contains("missing committed sequence") {
            Some(RunInvalidation::MissingSequence(difference.primary_key))
        } else if detail.contains("multiple times") || detail.contains("duplicate") {
            Some(RunInvalidation::DuplicateSequence(difference.primary_key))
        } else if detail.contains("hash mismatch") {
            Some(RunInvalidation::WorkloadHashMismatch)
        } else if detail.contains("reorder") {
            Some(RunInvalidation::StateChangingReorder {
                before: difference.expected_version,
                after: difference.actual_version,
            })
        } else if detail.contains("stale or mismatched LSN") {
            Some(RunInvalidation::StaleResult {
                target_lsn: difference.expected_version,
                visible_lsn: difference.actual_version,
            })
        } else if detail.contains("digest") {
            Some(RunInvalidation::ResultDigestMismatch {
                query: match difference.primary_key {
                    0 => QueryId::Q1,
                    1 => QueryId::Q2,
                    2 => QueryId::Q3,
                    3 => QueryId::Q4,
                    _ => QueryId::Q5,
                },
                checkpoint: difference.target_checkpoint,
            })
        } else {
            None
        };
        if let Some(reason) = reason {
            if !out.contains(&reason) {
                out.push(reason);
            }
        }
    }
    out
}

fn postgres_cell_text(row: &tokio_postgres::Row, index: usize) -> Result<Option<String>> {
    use tokio_postgres::types::Type;

    let ty = row.columns()[index].type_();
    if *ty == Type::TEXT || *ty == Type::VARCHAR || *ty == Type::BPCHAR || *ty == Type::NAME {
        return Ok(row.try_get::<_, Option<String>>(index)?);
    }
    if *ty == Type::INT8 {
        return Ok(row
            .try_get::<_, Option<i64>>(index)?
            .map(|value| value.to_string()));
    }
    if *ty == Type::INT4 {
        return Ok(row
            .try_get::<_, Option<i32>>(index)?
            .map(|value| value.to_string()));
    }
    if *ty == Type::INT2 {
        return Ok(row
            .try_get::<_, Option<i16>>(index)?
            .map(|value| value.to_string()));
    }
    if *ty == Type::BOOL {
        return Ok(row
            .try_get::<_, Option<bool>>(index)?
            .map(|value| value.to_string()));
    }
    anyhow::bail!(
        "unsupported PostgreSQL checkpoint column type {} for {}",
        ty.name(),
        row.columns()[index].name()
    )
}

async fn capture_postgres_samples(
    transaction: &tokio_postgres::Transaction<'_>,
    requested: &[RowSample],
) -> Result<Vec<RowSample>> {
    let mut requested_by_table: BTreeMap<&str, Vec<&RowSample>> = BTreeMap::new();
    for sample in requested {
        requested_by_table
            .entry(sample.table.as_str())
            .or_default()
            .push(sample);
    }
    let mut captured = Vec::with_capacity(requested.len());
    for (table, samples) in requested_by_table {
        let ids = samples
            .iter()
            .map(|sample| sample.primary_key.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let (primary_key, sql) = match table {
            "r1.tenants" => (
                "tenant_id",
                format!(
                    "SELECT tenant_id::text, region FROM r1.tenants WHERE tenant_id IN ({ids})"
                ),
            ),
            "r1.customers" => (
                "customer_id",
                format!(
                    "SELECT customer_id::text, tenant_id::text, segment, email_domain, profile::text, floor(extract(epoch FROM created_at) * 1000000)::bigint::text FROM r1.customers WHERE customer_id IN ({ids})"
                ),
            ),
            "r1.orders" => (
                "order_id",
                format!(
                    "SELECT order_id::text, tenant_id::text, customer_id::text, status, channel, amount_cents::text, floor(extract(epoch FROM created_at) * 1000000)::bigint::text, floor(extract(epoch FROM updated_at) * 1000000)::bigint::text, attributes::text FROM r1.orders WHERE order_id IN ({ids})"
                ),
            ),
            "r1.order_events" => (
                "event_id",
                format!(
                    "SELECT event_id::text, order_id::text, tenant_id::text, event_type, floor(extract(epoch FROM event_at) * 1000000)::bigint::text, metadata::text FROM r1.order_events WHERE event_id IN ({ids})"
                ),
            ),
            _ => anyhow::bail!("unsupported primary-key sample table {table}"),
        };
        let mut rows_by_id = BTreeMap::new();
        for row in transaction.query(&sql, &[]).await? {
            let values: Vec<String> = (0..row.columns().len())
                .map(|index| postgres_cell_text(&row, index).map(|value| value.unwrap_or_default()))
                .collect::<Result<_>>()?;
            let id = values[0]
                .parse::<u64>()
                .map_err(|error| anyhow::anyhow!("invalid {primary_key} sample: {error}"))?;
            rows_by_id.insert(id, values);
        }
        for expected in samples {
            let Some(values) = rows_by_id.remove(&expected.primary_key) else {
                // PostgreSQL represents an expected delete as absence. Retain
                // the tombstone evidence rather than silently dropping it.
                if expected.deleted {
                    captured.push((*expected).clone());
                }
                continue;
            };
            let values = match table {
                "r1.tenants" => BTreeMap::from([
                    ("tenant_id".into(), values[0].clone()),
                    ("region".into(), values[1].clone()),
                ]),
                "r1.customers" => BTreeMap::from([
                    ("customer_id".into(), values[0].clone()),
                    ("tenant_id".into(), values[1].clone()),
                    ("segment".into(), values[2].clone()),
                    ("email_domain".into(), values[3].clone()),
                    ("profile".into(), values[4].clone()),
                    ("created_at_micros".into(), values[5].clone()),
                ]),
                "r1.orders" => BTreeMap::from([
                    ("order_id".into(), values[0].clone()),
                    ("tenant_id".into(), values[1].clone()),
                    ("customer_id".into(), values[2].clone()),
                    ("status".into(), values[3].clone()),
                    ("channel".into(), values[4].clone()),
                    ("amount_cents".into(), values[5].clone()),
                    ("created_at_micros".into(), values[6].clone()),
                    ("updated_at_micros".into(), values[7].clone()),
                    ("attributes".into(), values[8].clone()),
                ]),
                "r1.order_events" => BTreeMap::from([
                    ("event_id".into(), values[0].clone()),
                    ("order_id".into(), values[1].clone()),
                    ("tenant_id".into(), values[2].clone()),
                    ("event_type".into(), values[3].clone()),
                    ("event_at_micros".into(), values[4].clone()),
                    ("metadata".into(), values[5].clone()),
                ]),
                _ => unreachable!("validated above"),
            };
            captured.push(RowSample {
                table: table.to_string(),
                primary_key: expected.primary_key,
                version: expected.version,
                deleted: false,
                values,
            });
        }
    }
    captured.sort_by(|left, right| {
        (left.table.as_str(), left.primary_key).cmp(&(right.table.as_str(), right.primary_key))
    });
    Ok(captured)
}

fn compare_samples(
    expected: &[RowSample],
    actual: &[RowSample],
    checkpoint: u64,
    engine: &str,
    out: &mut Vec<RowDifference>,
) {
    let expected: BTreeMap<_, _> = expected
        .iter()
        .map(|sample| ((sample.table.as_str(), sample.primary_key), sample))
        .collect();
    let actual: BTreeMap<_, _> = actual
        .iter()
        .map(|sample| ((sample.table.as_str(), sample.primary_key), sample))
        .collect();
    let keys: std::collections::BTreeSet<_> = expected.keys().chain(actual.keys()).collect();
    for key in keys {
        let expected = expected.get(key);
        let actual = actual.get(key);
        if expected != actual {
            out.push(RowDifference {
                table: key.0.to_string(),
                primary_key: key.1,
                expected_version: expected.map(|sample| sample.version).unwrap_or(0),
                actual_version: actual.map(|sample| sample.version).unwrap_or(0),
                target_checkpoint: checkpoint,
                detail: format!("engine {engine} primary-key sample mismatch"),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::{OrderEventRow, OrderRow};
    use std::sync::Mutex;

    fn plan(sequence: u64, operation: Operation) -> TransactionPlan {
        let operations = vec![operation];
        let bytes = serde_json::to_vec(&operations).unwrap();
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(bytes);
        TransactionPlan {
            sequence,
            logical_time_micros: (sequence * 1_000) as i64,
            operations,
            operation_sha256: digest.iter().map(|b| format!("{b:02x}")).collect(),
        }
    }

    const BASE_MICROS: i64 = 1_609_459_200_000_000;

    fn order(
        order_id: u64,
        tenant_id: u64,
        customer_id: u64,
        status: &str,
        amount: i64,
    ) -> Operation {
        Operation::InsertOrder(OrderRow {
            order_id,
            tenant_id,
            customer_id,
            status: status.into(),
            channel: "web".into(),
            amount_cents: amount,
            created_at_micros: BASE_MICROS,
            updated_at_micros: BASE_MICROS,
            attributes_json: "{}".into(),
        })
    }

    /// Five single-row commits: two inserts, one update, one event insert, one delete.
    pub struct OracleFixture {
        pub oracle: LedgerOracle,
        plans: Vec<TransactionPlan>,
    }

    impl OracleFixture {
        pub fn five_commits() -> Self {
            let mut oracle = LedgerOracle::new();
            oracle.load_tenant(1, "eu");
            oracle.load_tenant(2, "us");
            let plans = vec![
                plan(1, order(100, 1, 10, "new", 1_000)),
                plan(2, order(200, 2, 20, "paid", 2_000)),
                plan(
                    3,
                    Operation::UpdateOrder {
                        order_id: 100,
                        tenant_id: 1,
                        customer_id: 10,
                        status: "paid".into(),
                        channel: "app".into(),
                        amount_cents: 1_500,
                        created_at_micros: BASE_MICROS,
                        updated_at_micros: BASE_MICROS + 5_000_000,
                        attributes_json: "{}".into(),
                    },
                ),
                plan(
                    4,
                    Operation::InsertOrderEvent(OrderEventRow {
                        event_id: 300,
                        order_id: 100,
                        tenant_id: 1,
                        event_type: "shipped".into(),
                        event_at_micros: BASE_MICROS + 60_000_000,
                        metadata_json: "{}".into(),
                    }),
                ),
                plan(
                    5,
                    Operation::DeleteOrder {
                        order_id: 200,
                        tenant_id: 2,
                    },
                ),
            ];
            for (i, p) in plans.iter().enumerate() {
                oracle.apply(p, 100 + i as u64).unwrap();
            }
            Self { oracle, plans }
        }

        /// Applies exactly one corruption to a fresh candidate oracle.
        pub fn mutated(&self, mutation: Mutation) -> LedgerOracle {
            let mut candidate = LedgerOracle::new();
            candidate.load_tenant(1, "eu");
            candidate.load_tenant(2, "us");
            for (i, p) in self.plans.iter().enumerate() {
                let lsn = 100 + i as u64;
                match mutation {
                    Mutation::DropSequence(3) if p.sequence == 3 => continue,
                    Mutation::DuplicateSequence(4) if p.sequence == 4 => {
                        candidate.apply_unchecked(p, lsn).unwrap();
                        candidate.apply_unchecked(p, lsn).unwrap();
                        continue;
                    }
                    Mutation::UseVersionBeforeCheckpoint if p.sequence == 3 => {
                        // Apply the transaction but keep the pre-update value
                        // visible: the candidate never advanced order 100's state.
                        candidate.apply_unchecked(p, lsn).unwrap();
                        let stale = candidate.order_history[&100][0].1.clone();
                        candidate
                            .order_history
                            .get_mut(&100)
                            .unwrap()
                            .last_mut()
                            .unwrap()
                            .1 = stale;
                        candidate
                            .orders
                            .insert(100, candidate.orders_at(lsn)[&100].clone());
                        continue;
                    }
                    Mutation::IgnoreLatestTombstone if p.sequence == 5 => {
                        // Delete acknowledged but the row stays alive.
                        candidate.apply_unchecked(p, lsn).unwrap();
                        let resurrected = OrderState {
                            tenant_id: 2,
                            customer_id: 20,
                            status: "paid".into(),
                            channel: "web".into(),
                            amount_cents: 2_000,
                            created_at_micros: BASE_MICROS,
                            updated_at_micros: BASE_MICROS,
                            attributes_json: "{}".into(),
                            version: lsn,
                        };
                        candidate.orders.insert(200, resurrected.clone());
                        candidate
                            .order_history
                            .get_mut(&200)
                            .unwrap()
                            .last_mut()
                            .unwrap()
                            .1 = Some(resurrected);
                        continue;
                    }
                    _ => candidate.apply_unchecked(p, lsn).unwrap(),
                }
            }
            candidate
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Mutation {
        DropSequence(u64),
        DuplicateSequence(u64),
        UseVersionBeforeCheckpoint,
        IgnoreLatestTombstone,
    }

    #[test]
    fn oracle_rejects_each_required_corruption_class() {
        let fixture = OracleFixture::five_commits();
        for mutation in [
            Mutation::DropSequence(3),
            Mutation::DuplicateSequence(4),
            Mutation::UseVersionBeforeCheckpoint,
            Mutation::IgnoreLatestTombstone,
        ] {
            let candidate = fixture.mutated(mutation);
            let verdict = fixture.oracle.compare(&candidate);
            assert!(!verdict.passed, "mutation must fail: {mutation:?}");
            assert!(
                !verdict.differences.is_empty(),
                "mutation must report differences: {mutation:?}"
            );
        }
    }

    #[test]
    fn clean_candidate_passes_and_queries_match() {
        let fixture = OracleFixture::five_commits();
        let mut clean = LedgerOracle::new();
        clean.load_tenant(1, "eu");
        clean.load_tenant(2, "us");
        for (i, p) in fixture.plans.iter().enumerate() {
            clean.apply(p, 100 + i as u64).unwrap();
        }
        assert!(fixture.oracle.compare(&clean).passed);

        let params = QueryParameters {
            window_end_micros: BASE_MICROS + 7 * DAY_MICROS,
            tenant_id: 1,
            tenant_set: vec![1, 2],
        };
        let target = fixture.oracle.latest_checkpoint();
        for (_, id) in QUERIES {
            let a = fixture.oracle.query_at(id, &params, target);
            let b = clean.query_at(id, &params, target);
            assert_eq!(
                canonical_digest(&a),
                canonical_digest(&b),
                "clean oracle must agree on {id:?}"
            );
        }
    }

    #[test]
    fn q1_through_q5_return_hand_checked_canonical_integer_rows() {
        let fixture = OracleFixture::five_commits();
        let params = QueryParameters {
            window_end_micros: BASE_MICROS + 7 * DAY_MICROS,
            tenant_id: 1,
            tenant_set: vec![1, 2],
        };
        let target = fixture.oracle.latest_checkpoint();
        let expected = [
            (
                QueryId::Q1,
                vec!["customer_id", "sum(amount_cents)", "count(*)"],
                vec![vec!["10", "1500", "1"]],
            ),
            (
                QueryId::Q2,
                vec!["status", "count(*)"],
                vec![vec!["paid", "1"]],
            ),
            (
                QueryId::Q3,
                vec![
                    "region",
                    "channel",
                    "status",
                    "sum(amount_cents)",
                    "count(*)",
                ],
                vec![vec!["eu", "app", "paid", "1500", "1"]],
            ),
            (QueryId::Q4, vec!["event_type", "count(*)"], vec![]),
            (
                QueryId::Q5,
                vec!["status", "count(*)", "sum(amount_cents)"],
                vec![vec!["paid", "1", "1500"]],
            ),
        ];
        for (query, columns, rows) in expected {
            let actual = fixture.oracle.query_at(query, &params, target);
            let expected = crate::query::QueryResult {
                columns: columns.into_iter().map(str::to_string).collect(),
                rows: rows
                    .into_iter()
                    .map(|row| row.into_iter().map(|cell| Some(cell.to_string())).collect())
                    .collect(),
            };
            assert_eq!(
                canonical_digest(&actual),
                canonical_digest(&expected),
                "{query:?}"
            );
        }
    }

    #[test]
    fn update_and_delete_are_visible_at_target_lsn() {
        let fixture = OracleFixture::five_commits();
        let params = QueryParameters {
            window_end_micros: BASE_MICROS + 7 * DAY_MICROS,
            tenant_id: 1,
            tenant_set: vec![1, 2],
        };
        // Before the update (LSN 101): order 100 is "new" with 1000 cents.
        let q5_before = fixture.oracle.query_at(QueryId::Q5, &params, 101);
        let flat: Vec<(String, String, String)> = q5_before
            .rows
            .iter()
            .map(|r| {
                (
                    r[0].clone().unwrap(),
                    r[1].clone().unwrap(),
                    r[2].clone().unwrap(),
                )
            })
            .collect();
        assert!(flat.contains(&("new".into(), "1".into(), "1000".into())));
        // After everything (LSN 104): order 100 is "paid"/1500, order 200 deleted.
        let q5_after = fixture.oracle.query_at(QueryId::Q5, &params, 104);
        let flat: Vec<(String, String, String)> = q5_after
            .rows
            .iter()
            .map(|r| {
                (
                    r[0].clone().unwrap(),
                    r[1].clone().unwrap(),
                    r[2].clone().unwrap(),
                )
            })
            .collect();
        assert!(flat.contains(&("paid".into(), "1".into(), "1500".into())));
        assert!(
            !flat.iter().any(|r| r.2 == "2000"),
            "deleted order must be gone"
        );
        let tombstone = fixture
            .oracle
            .sample_rows_at(104, 16)
            .into_iter()
            .find(|sample| sample.table == "r1.orders" && sample.primary_key == 200)
            .expect("deterministic samples must retain the latest order tombstone");
        assert!(tombstone.deleted);
        assert_eq!(tombstone.version, 104);
    }

    #[test]
    fn apply_rejects_gaps_and_hash_changes() {
        let p1 = plan(1, order(1, 1, 1, "new", 10));
        // --- gap test ---
        let mut oracle_gap = LedgerOracle::new();
        oracle_gap.apply(&p1, 100).unwrap();
        let gap_fails = oracle_gap
            .apply(&plan(3, order(3, 1, 1, "new", 10)), 102)
            .is_err();
        assert!(gap_fails, "applying sequence 3 after 1 must fail (gap)");
        // --- hash mismatch test (re-apply with different hash) ---
        let mut oracle_hash = LedgerOracle::new();
        oracle_hash.apply(&p1, 100).unwrap();
        let mut bad_hash_plan = plan(1, order(1, 1, 1, "new", 10));
        bad_hash_plan.operation_sha256 = "different_hash".into();
        let hash_fails = oracle_hash.apply(&bad_hash_plan, 100).is_err();
        assert!(
            hash_fails,
            "re-applying plan 1 with a different hash must fail"
        );
    }

    #[test]
    fn apply_rejects_tampered_operations_and_non_increasing_commit_lsns() {
        // This catches accepting a ledger marker whose hash no longer matches
        // the committed operation bytes, or collapsing two commits onto one
        // source checkpoint.
        let mut tampered = plan(1, order(1, 1, 1, "new", 10));
        tampered.operations = vec![order(1, 1, 1, "paid", 99)];
        let mut oracle = LedgerOracle::new();
        assert!(oracle.apply(&tampered, 100).is_err());

        let p1 = plan(1, order(1, 1, 1, "new", 10));
        let p2 = plan(2, order(2, 1, 1, "paid", 20));
        oracle.apply(&p1, 100).unwrap();
        assert!(oracle.apply(&p2, 100).is_err());
    }

    #[test]
    fn compare_rejects_immutable_tenant_state_changes() {
        // Tenant dimensions are part of Q3; a candidate with a different
        // region must never pass just because its order rows are identical.
        let fixture = OracleFixture::five_commits();
        let mut candidate = fixture.mutated(Mutation::DuplicateSequence(99));
        candidate.tenants.get_mut(&1).unwrap().region = "apac".into();
        let verdict = fixture.oracle.compare(&candidate);
        assert!(!verdict.passed);
        assert!(verdict
            .differences
            .iter()
            .any(|difference| difference.table == "r1.tenants"));
    }

    #[test]
    fn deterministic_samples_reconstruct_customer_history_at_each_checkpoint() {
        // This catches a current-state-only customer map: exact checkpoint
        // samples must retain the version before and after an update.
        let mut oracle = LedgerOracle::new();
        let customer = Operation::InsertCustomer(crate::workload::CustomerRow {
            customer_id: 7,
            tenant_id: 1,
            segment: "consumer".into(),
            email_domain: "example.test".into(),
            profile_json: "{\"tier\":1}".into(),
            created_at_micros: BASE_MICROS,
        });
        oracle.apply(&plan(1, customer), 100).unwrap();
        oracle
            .apply(
                &plan(
                    2,
                    Operation::UpdateCustomer {
                        customer_id: 7,
                        tenant_id: 1,
                        segment: "enterprise".into(),
                        email_domain: "corp.test".into(),
                        profile_json: "{\"tier\":2}".into(),
                        created_at_micros: BASE_MICROS,
                    },
                ),
                101,
            )
            .unwrap();

        let before = oracle.sample_rows_at(100, 16);
        let after = oracle.sample_rows_at(101, 16);
        let before_customer = before
            .iter()
            .find(|sample| sample.table == "r1.customers" && sample.primary_key == 7)
            .unwrap();
        let after_customer = after
            .iter()
            .find(|sample| sample.table == "r1.customers" && sample.primary_key == 7)
            .unwrap();
        assert_eq!(before_customer.values["segment"], "consumer");
        assert_eq!(after_customer.values["segment"], "enterprise");
        assert_eq!(before_customer.version, 100);
        assert_eq!(after_customer.version, 101);
    }

    #[test]
    fn compare_at_rejects_a_candidate_with_a_wrong_historic_customer_version() {
        let fixture = OracleFixture::five_commits();
        let mut candidate = fixture.mutated(Mutation::DuplicateSequence(99));
        // Sequence 3 does not touch customers, so alter the historic version
        // directly while retaining the latest map. A current-state comparison
        // would miss this corruption at an earlier checkpoint.
        let customer = CustomerState {
            tenant_id: 1,
            segment: "wrong".into(),
            email_domain: "wrong.test".into(),
            profile_json: "{}".into(),
            created_at_micros: BASE_MICROS,
            version: 100,
        };
        candidate.customers.insert(10, customer.clone());
        candidate
            .customer_history
            .insert(10, vec![(100, Some(customer))]);
        let verdict = fixture.oracle.compare_at(&candidate, 101);
        assert!(!verdict.passed);
        assert!(verdict
            .differences
            .iter()
            .any(|difference| difference.table == "r1.customers"));
    }

    #[test]
    fn compare_at_rejects_disjoint_row_lsn_reorder_with_typed_invalidation() {
        let fixture = OracleFixture::five_commits();
        let mut candidate = fixture.mutated(Mutation::DuplicateSequence(99));
        candidate.sequence_lsn.insert(1, 101);
        candidate.sequence_lsn.insert(2, 100);
        let verdict = fixture.oracle.compare_at(&candidate, 104);
        assert!(!verdict.passed);
        assert!(verdict.invalidations.iter().any(|reason| matches!(
            reason,
            RunInvalidation::StateChangingReorder {
                before: 1,
                after: 2
            }
        )));
    }

    #[test]
    fn ledger_digest_mismatch_maps_to_a_durable_digest_invalidation() {
        let fixture = OracleFixture::five_commits();
        let params = QueryParameters {
            window_end_micros: BASE_MICROS + 7 * DAY_MICROS,
            tenant_id: 1,
            tenant_set: vec![1, 2],
        };
        let digests =
            ledger_query_digests(&fixture.oracle, &params, fixture.oracle.latest_checkpoint());
        let difference = RowDifference {
            table: "postgres".into(),
            primary_key: QueryId::Q1 as u64,
            expected_version: 104,
            actual_version: 104,
            target_checkpoint: 104,
            detail: "q1 PostgreSQL digest disagrees with ledger oracle".into(),
        };
        assert_ne!(digests["q1"], "deliberately-wrong");
        assert_eq!(
            invalidations_for(&[difference]),
            vec![RunInvalidation::ResultDigestMismatch {
                query: QueryId::Q1,
                checkpoint: 104
            }]
        );
    }

    struct RecordingWriter {
        events: Mutex<Vec<&'static str>>,
        fail_drain: bool,
    }

    #[async_trait::async_trait]
    impl WriterControl for RecordingWriter {
        async fn pause(&self) -> Result<()> {
            self.events.lock().unwrap().push("pause");
            Ok(())
        }

        async fn drain(&self) -> Result<()> {
            self.events.lock().unwrap().push("drain");
            if self.fail_drain {
                anyhow::bail!("drain failed")
            }
            Ok(())
        }

        async fn resume(&self) -> Result<()> {
            self.events.lock().unwrap().push("resume");
            Ok(())
        }
    }

    #[tokio::test]
    async fn checkpoint_resumes_writer_after_every_post_pause_error() {
        // This catches an error path that leaves the source paused after drain
        // or checkpoint capture has failed.
        let drain_failure = RecordingWriter {
            events: Mutex::new(Vec::new()),
            fail_drain: true,
        };
        assert!(
            PostgresCheckpoint::with_paused_writer(&drain_failure, async {
                Ok::<_, anyhow::Error>(())
            })
            .await
            .is_err()
        );
        assert_eq!(
            *drain_failure.events.lock().unwrap(),
            ["pause", "drain", "resume"]
        );

        let capture_failure = RecordingWriter {
            events: Mutex::new(Vec::new()),
            fail_drain: false,
        };
        assert!(
            PostgresCheckpoint::with_paused_writer(&capture_failure, async {
                Err::<(), _>(anyhow::anyhow!("capture failed"))
            })
            .await
            .is_err()
        );
        assert_eq!(
            *capture_failure.events.lock().unwrap(),
            ["pause", "drain", "resume"]
        );
    }
}
