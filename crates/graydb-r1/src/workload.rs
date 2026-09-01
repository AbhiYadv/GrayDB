use crate::generator::{mix64, Table};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Mutex;
use std::time::Duration;

const OPERATION_CYCLE_ROWS: u64 = 50;
const INSERT_ROWS_PER_CYCLE: u64 = 45;
const UPDATE_ROWS_PER_CYCLE: u64 = 4;
const DELETE_ROWS_PER_CYCLE: u64 = 1;
const INSERT_DISTRIBUTION_CYCLE: u64 = 20;
const UPDATE_DISTRIBUTION_CYCLE: u64 = 10;
const DELETE_DISTRIBUTION_CYCLE: u64 = 5;
const APP_CUSTOMER_BASE: u64 = 10_000_000_000;
const APP_ORDER_BASE: u64 = 20_000_000_000;
const APP_EVENT_BASE: u64 = 30_000_000_000;
const FIXTURE_TENANT_COUNT: u64 = 64;
const FIXTURE_CUSTOMERS_PER_TENANT: u64 = 5;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomerRow {
    pub customer_id: u64,
    pub tenant_id: u64,
    pub segment: String,
    pub email_domain: String,
    pub profile_json: String,
    pub created_at_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRow {
    pub order_id: u64,
    pub tenant_id: u64,
    pub customer_id: u64,
    pub status: String,
    pub channel: String,
    pub amount_cents: i64,
    pub created_at_micros: i64,
    pub updated_at_micros: i64,
    pub attributes_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderEventRow {
    pub event_id: u64,
    pub order_id: u64,
    pub tenant_id: u64,
    pub event_type: String,
    pub event_at_micros: i64,
    pub metadata_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    InsertCustomer(CustomerRow),
    InsertOrder(OrderRow),
    InsertOrderEvent(OrderEventRow),
    UpdateCustomer {
        customer_id: u64,
        tenant_id: u64,
        segment: String,
        email_domain: String,
        profile_json: String,
        created_at_micros: i64,
    },
    UpdateOrder {
        order_id: u64,
        tenant_id: u64,
        customer_id: u64,
        status: String,
        channel: String,
        amount_cents: i64,
        created_at_micros: i64,
        updated_at_micros: i64,
        attributes_json: String,
    },
    DeleteOrder {
        order_id: u64,
        tenant_id: u64,
    },
    DeleteOrderEvent {
        event_id: u64,
        order_id: u64,
        tenant_id: u64,
    },
}

impl Operation {
    pub fn is_insert(&self) -> bool {
        matches!(
            self,
            Self::InsertCustomer(_) | Self::InsertOrder(_) | Self::InsertOrderEvent(_)
        )
    }

    pub fn is_update(&self) -> bool {
        matches!(self, Self::UpdateCustomer { .. } | Self::UpdateOrder { .. })
    }

    pub fn is_delete(&self) -> bool {
        matches!(
            self,
            Self::DeleteOrder { .. } | Self::DeleteOrderEvent { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionPlan {
    pub sequence: u64,
    pub logical_time_micros: i64,
    pub operations: Vec<Operation>,
    pub operation_sha256: String,
}

#[derive(Debug)]
pub struct WorkloadPlanner {
    seed: u64,
    prefix_rows: Mutex<Vec<u64>>,
}

impl WorkloadPlanner {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            prefix_rows: Mutex::new(vec![0]),
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn plan(&self, sequence: u64) -> TransactionPlan {
        let size = transaction_size(self.seed, sequence);
        let offset = self.rows_before_sequence(sequence);
        let mut operations = Vec::with_capacity(size);
        for i in 0..size {
            operations.push(plan_operation(self.seed, offset + i as u64));
        }
        let bytes = serde_json::to_vec(&operations).expect("operation serialization");
        let digest = Sha256::digest(bytes);
        TransactionPlan {
            sequence,
            logical_time_micros: logical_time_micros(offset),
            operations,
            operation_sha256: encode_hex(&digest),
        }
    }

    fn rows_before_sequence(&self, sequence: u64) -> u64 {
        let mut prefix_rows = self.prefix_rows.lock().expect("planner cache poisoned");
        while prefix_rows.len() <= sequence as usize {
            let next_sequence = prefix_rows.len() as u64;
            let next_total = prefix_rows.last().copied().unwrap_or_default()
                + transaction_size(self.seed, next_sequence) as u64;
            prefix_rows.push(next_total);
        }
        prefix_rows[sequence.saturating_sub(1) as usize]
    }
}

fn transaction_size(seed: u64, sequence: u64) -> usize {
    match mix64(seed ^ sequence.rotate_left(17)) % 10_000 {
        0..=9_499 => 1,
        9_500..=9_899 => 10,
        9_900..=9_989 => 100,
        9_990..=9_999 => 1_000,
        _ => unreachable!("mod 10_000 stays within 0..=9_999"),
    }
}

fn plan_operation(seed: u64, global_ordinal: u64) -> Operation {
    let cycle = global_ordinal / OPERATION_CYCLE_ROWS;
    let slot = global_ordinal % OPERATION_CYCLE_ROWS;
    if slot < INSERT_ROWS_PER_CYCLE {
        let insert_index = cycle * INSERT_ROWS_PER_CYCLE + slot;
        plan_insert(seed, insert_index)
    } else if slot < INSERT_ROWS_PER_CYCLE + UPDATE_ROWS_PER_CYCLE {
        let update_index = cycle * UPDATE_ROWS_PER_CYCLE + (slot - INSERT_ROWS_PER_CYCLE);
        plan_update(seed, update_index, global_ordinal)
    } else {
        let delete_index =
            cycle * DELETE_ROWS_PER_CYCLE + (slot - INSERT_ROWS_PER_CYCLE - UPDATE_ROWS_PER_CYCLE);
        plan_delete(seed, delete_index)
    }
}

fn plan_insert(seed: u64, insert_index: u64) -> Operation {
    match insert_index % INSERT_DISTRIBUTION_CYCLE {
        0..=11 => Operation::InsertOrderEvent(make_insert_event(seed, insert_index)),
        12..=18 => Operation::InsertOrder(make_insert_order(seed, insert_index)),
        19 => Operation::InsertCustomer(make_insert_customer(seed, insert_index)),
        _ => unreachable!("bounded insert distribution"),
    }
}

fn plan_update(seed: u64, update_index: u64, global_ordinal: u64) -> Operation {
    match update_index % UPDATE_DISTRIBUTION_CYCLE {
        0..=8 => {
            let order_insert_count = insert_target_count_before(global_ordinal, Table::Orders);
            let order_index = deterministic_target_index(seed, update_index, order_insert_count);
            let row = make_insert_order(seed, order_index);
            Operation::UpdateOrder {
                order_id: row.order_id,
                tenant_id: row.tenant_id,
                customer_id: row.customer_id,
                status: next_status(seed, update_index),
                channel: row.channel,
                amount_cents: mutate_amount(seed, update_index, row.amount_cents),
                created_at_micros: row.created_at_micros,
                updated_at_micros: row.updated_at_micros
                    + 1_000_000
                    + (update_index as i64 % 10_000),
                attributes_json: mutate_json("order-update", seed, update_index),
            }
        }
        9 => {
            let customer_insert_count =
                insert_target_count_before(global_ordinal, Table::Customers);
            let customer_index =
                deterministic_target_index(seed, update_index, customer_insert_count);
            let row = make_insert_customer(seed, customer_index);
            Operation::UpdateCustomer {
                customer_id: row.customer_id,
                tenant_id: row.tenant_id,
                segment: next_segment(seed, update_index),
                email_domain: row.email_domain,
                profile_json: mutate_json("customer-update", seed, update_index),
                created_at_micros: row.created_at_micros,
            }
        }
        _ => unreachable!("bounded update distribution"),
    }
}

fn plan_delete(seed: u64, delete_index: u64) -> Operation {
    match delete_index % DELETE_DISTRIBUTION_CYCLE {
        0..=3 => {
            let row = make_insert_order(seed, delete_index);
            Operation::DeleteOrder {
                order_id: row.order_id,
                tenant_id: row.tenant_id,
            }
        }
        4 => {
            let row = make_insert_event(seed, delete_index);
            Operation::DeleteOrderEvent {
                event_id: row.event_id,
                order_id: row.order_id,
                tenant_id: row.tenant_id,
            }
        }
        _ => unreachable!("bounded delete distribution"),
    }
}

fn insert_target_count_before(global_ordinal: u64, table: Table) -> u64 {
    let insert_rows_before = insert_rows_before(global_ordinal);
    match table {
        Table::Tenants => 0,
        Table::Customers => insert_rows_before / INSERT_DISTRIBUTION_CYCLE,
        Table::Orders => {
            (insert_rows_before / INSERT_DISTRIBUTION_CYCLE) * 7
                + ((insert_rows_before % INSERT_DISTRIBUTION_CYCLE).saturating_sub(12)).min(7)
        }
        Table::OrderEvents => {
            (insert_rows_before / INSERT_DISTRIBUTION_CYCLE) * 12
                + (insert_rows_before % INSERT_DISTRIBUTION_CYCLE).min(12)
        }
    }
}

fn insert_rows_before(global_ordinal: u64) -> u64 {
    let full_cycles = global_ordinal / OPERATION_CYCLE_ROWS;
    let cycle_rows = global_ordinal % OPERATION_CYCLE_ROWS;
    full_cycles * INSERT_ROWS_PER_CYCLE + cycle_rows.min(INSERT_ROWS_PER_CYCLE)
}

fn deterministic_target_index(seed: u64, sequence_index: u64, available: u64) -> u64 {
    if available == 0 {
        0
    } else {
        mix64(seed ^ sequence_index.rotate_left(11)) % available
    }
}

fn make_insert_customer(seed: u64, customer_index: u64) -> CustomerRow {
    let customer_id = APP_CUSTOMER_BASE + customer_index + 1;
    let tenant_id = tenant_for_app_index(seed, customer_index, 17);
    let created_at_micros = logical_time_micros(customer_index * 20 + 19);
    CustomerRow {
        customer_id,
        tenant_id,
        segment: next_segment(seed, customer_index),
        email_domain: next_email_domain(seed, customer_index),
        profile_json: bounded_json("customer", seed, customer_index),
        created_at_micros,
    }
}

fn make_insert_order(seed: u64, order_index: u64) -> OrderRow {
    let order_id = APP_ORDER_BASE + order_index + 1;
    let customer_slot =
        mix64(seed ^ order_index.rotate_left(7) ^ 0x0c55_7a55) % FIXTURE_CUSTOMERS_PER_TENANT;
    let tenant_slot =
        mix64(seed ^ order_index.rotate_left(13) ^ 0x51de_0cc1) % FIXTURE_TENANT_COUNT;
    let tenant_id = tenant_slot * 100 + 1;
    let customer_id = tenant_id + customer_slot + 1;
    let created_at_micros = logical_time_micros(order_index * 3);
    OrderRow {
        order_id,
        tenant_id,
        customer_id,
        status: next_status(seed, order_index),
        channel: next_channel(seed, order_index),
        amount_cents: base_amount(seed, order_index),
        created_at_micros,
        updated_at_micros: created_at_micros + 250_000,
        attributes_json: bounded_json("order", seed, order_index),
    }
}

fn make_insert_event(seed: u64, event_index: u64) -> OrderEventRow {
    let event_id = APP_EVENT_BASE + event_index + 1;
    let order_insert_index =
        deterministic_target_index(seed ^ 0x0e17_0e17, event_index, event_index + 1);
    let order = make_insert_order(seed, order_insert_index);
    OrderEventRow {
        event_id,
        order_id: order.order_id,
        tenant_id: order.tenant_id,
        event_type: next_event_type(seed, event_index),
        event_at_micros: logical_time_micros(event_index),
        metadata_json: bounded_json("event", seed, event_index),
    }
}

fn tenant_for_app_index(seed: u64, app_index: u64, salt: u64) -> u64 {
    let slot = mix64(seed ^ app_index.rotate_left(5) ^ salt) % FIXTURE_TENANT_COUNT;
    slot * 100 + 1
}

fn next_segment(seed: u64, n: u64) -> String {
    weighted_choice(
        mix64(seed ^ n.rotate_left(9)),
        &[
            ("consumer", 55),
            ("smb", 29),
            ("mid-market", 12),
            ("enterprise", 4),
        ],
    )
    .to_string()
}

fn next_email_domain(seed: u64, n: u64) -> String {
    weighted_choice(
        mix64(seed ^ n.rotate_left(3) ^ 0xe11a),
        &[
            ("example.com", 58),
            ("mail.test", 25),
            ("corp.test", 12),
            ("acme.test", 5),
        ],
    )
    .to_string()
}

fn next_status(seed: u64, n: u64) -> String {
    weighted_choice(
        mix64(seed ^ n.rotate_left(19) ^ 0x57a7),
        &[
            ("paid", 46),
            ("shipped", 27),
            ("pending", 15),
            ("cancelled", 8),
            ("refunded", 4),
        ],
    )
    .to_string()
}

fn next_channel(seed: u64, n: u64) -> String {
    weighted_choice(
        mix64(seed ^ n.rotate_left(27) ^ 0x0ca1),
        &[("web", 61), ("mobile", 31), ("partner", 8)],
    )
    .to_string()
}

fn next_event_type(seed: u64, n: u64) -> String {
    weighted_choice(
        mix64(seed ^ n.rotate_left(31) ^ 0x0e47),
        &[
            ("created", 42),
            ("paid", 30),
            ("fulfilled", 17),
            ("cancelled", 7),
            ("returned", 4),
        ],
    )
    .to_string()
}

fn weighted_choice<'a, const N: usize>(draw: u64, choices: &'a [(&'a str, u64); N]) -> &'a str {
    let total = choices.iter().map(|(_, weight)| weight).sum::<u64>();
    let mut slot = draw % total;
    for (value, weight) in choices {
        if slot < *weight {
            return value;
        }
        slot -= *weight;
    }
    unreachable!("weighted choices are non-empty")
}

fn base_amount(seed: u64, n: u64) -> i64 {
    let bucket = mix64(seed ^ n.rotate_left(17) ^ 0xa11c);
    let value = mix64(seed ^ n.rotate_left(23) ^ 0xb00c);
    match bucket % 100 {
        0..=54 => 100 + (value % 2_400) as i64,
        55..=81 => 2_500 + (value % 7_500) as i64,
        82..=93 => 10_000 + (value % 40_000) as i64,
        94..=98 => 50_000 + (value % 200_000) as i64,
        _ => 250_000 + (value % 1_750_000) as i64,
    }
}

fn mutate_amount(seed: u64, n: u64, amount: i64) -> i64 {
    let delta = (mix64(seed ^ n.rotate_left(29) ^ 0xd31a) % 10_000) as i64;
    amount.saturating_add(delta)
}

fn bounded_json(kind: &str, seed: u64, n: u64) -> String {
    format!(
        "{{\"kind\":\"{kind}\",\"payload\":{},\"schema_version\":1}}",
        mix64(seed ^ n.rotate_left(37)) % 1_000
    )
}

fn mutate_json(kind: &str, seed: u64, n: u64) -> String {
    format!(
        "{{\"kind\":\"{kind}\",\"revision\":{},\"schema_version\":1}}",
        mix64(seed ^ n.rotate_left(41)) % 1_000
    )
}

fn logical_time_micros(global_ordinal: u64) -> i64 {
    1_700_000_000_000_000 + global_ordinal as i64 * 1_000
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct RowMix {
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
}

impl RowMix {
    pub fn from_plans(plans: &[TransactionPlan]) -> Self {
        plans.iter().flat_map(|plan| plan.operations.iter()).fold(
            Self::default(),
            |mut mix, operation| {
                if operation.is_insert() {
                    mix.inserts += 1;
                } else if operation.is_update() {
                    mix.updates += 1;
                } else {
                    mix.deletes += 1;
                }
                mix
            },
        )
    }

    fn fraction(part: u64, total: u64) -> f64 {
        if total == 0 {
            0.0
        } else {
            part as f64 / total as f64
        }
    }

    pub fn insert_fraction(&self) -> f64 {
        Self::fraction(self.inserts, self.total())
    }

    pub fn update_fraction(&self) -> f64 {
        Self::fraction(self.updates, self.total())
    }

    pub fn delete_fraction(&self) -> f64 {
        Self::fraction(self.deletes, self.total())
    }

    fn total(&self) -> u64 {
        self.inserts + self.updates + self.deletes
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct TableMix {
    pub insert_customers: u64,
    pub insert_orders: u64,
    pub insert_events: u64,
    pub update_customers: u64,
    pub update_orders: u64,
    pub delete_orders: u64,
    pub delete_events: u64,
}

impl TableMix {
    pub fn from_plans(plans: &[TransactionPlan]) -> Self {
        plans.iter().flat_map(|plan| plan.operations.iter()).fold(
            Self::default(),
            |mut mix, operation| {
                match operation {
                    Operation::InsertCustomer(_) => mix.insert_customers += 1,
                    Operation::InsertOrder(_) => mix.insert_orders += 1,
                    Operation::InsertOrderEvent(_) => mix.insert_events += 1,
                    Operation::UpdateCustomer { .. } => mix.update_customers += 1,
                    Operation::UpdateOrder { .. } => mix.update_orders += 1,
                    Operation::DeleteOrder { .. } => mix.delete_orders += 1,
                    Operation::DeleteOrderEvent { .. } => mix.delete_events += 1,
                }
                mix
            },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RateInterval {
    pub target_rows: u64,
    pub achieved_rows: u64,
}

#[derive(Debug)]
pub struct RateLimiter {
    rate: f64,
    capacity: f64,
    tokens: f64,
    last: tokio::time::Instant,
    interval_start: tokio::time::Instant,
    interval_target: u64,
    interval_achieved: u64,
    intervals: Vec<RateInterval>,
}

impl RateLimiter {
    pub fn new(rows_per_second: u64, max_transaction_rows: u64) -> Self {
        let now = tokio::time::Instant::now();
        Self {
            rate: rows_per_second as f64,
            capacity: max_transaction_rows as f64,
            tokens: max_transaction_rows as f64,
            last: now,
            interval_start: now,
            interval_target: 0,
            interval_achieved: 0,
            intervals: Vec::new(),
        }
    }

    pub async fn acquire(&mut self, rows: u64) -> Result<()> {
        if rows == 0 {
            return Ok(());
        }
        if rows as f64 > self.capacity {
            return Err(anyhow!(
                "rows {rows} exceed limiter burst {}",
                self.capacity as u64
            ));
        }
        self.roll_interval(tokio::time::Instant::now());
        self.interval_target = self.interval_target.saturating_add(rows);
        loop {
            self.roll_interval(tokio::time::Instant::now());
            if self.tokens >= rows as f64 {
                self.tokens -= rows as f64;
                self.interval_achieved = self.interval_achieved.saturating_add(rows);
                return Ok(());
            }
            let wait = (rows as f64 - self.tokens) / self.rate;
            tokio::time::sleep(Duration::from_secs_f64(wait.max(0.000_001))).await;
        }
    }

    pub fn intervals(&self) -> &[RateInterval] {
        &self.intervals
    }

    fn roll_interval(&mut self, now: tokio::time::Instant) {
        let elapsed = (now - self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
        self.last = now;
        while now - self.interval_start >= Duration::from_secs(60) {
            self.intervals.push(RateInterval {
                target_rows: self.interval_target,
                achieved_rows: self.interval_achieved,
            });
            self.interval_target = 0;
            self.interval_achieved = 0;
            self.interval_start += Duration::from_secs(60);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_is_reproducible_and_converges_on_row_mix() {
        let planner = WorkloadPlanner::new(20260901);
        let a: Vec<_> = (1..=20_000)
            .map(|sequence| planner.plan(sequence))
            .collect();
        let b: Vec<_> = (1..=20_000)
            .map(|sequence| planner.plan(sequence))
            .collect();
        assert_eq!(a, b);
        let mix = RowMix::from_plans(&a);
        eprintln!(
            "mix {}/{}/{} = {:.4}/{:.4}/{:.4}",
            mix.inserts,
            mix.updates,
            mix.deletes,
            mix.insert_fraction(),
            mix.update_fraction(),
            mix.delete_fraction()
        );
        assert!((mix.insert_fraction() - 0.90).abs() <= 0.005);
        assert!((mix.update_fraction() - 0.08).abs() <= 0.005);
        assert!((mix.delete_fraction() - 0.02).abs() <= 0.005);
    }

    #[test]
    fn transaction_size_distribution_is_frozen() {
        let planner = WorkloadPlanner::new(20260901);
        let mut ones = 0_u64;
        let mut tens = 0_u64;
        let mut hundreds = 0_u64;
        let mut thousands = 0_u64;
        for sequence in 1..=50_000 {
            match planner.plan(sequence).operations.len() {
                1 => ones += 1,
                10 => tens += 1,
                100 => hundreds += 1,
                1_000 => thousands += 1,
                size => panic!("unexpected transaction size {size}"),
            }
        }
        assert!((ones as f64 / 50_000.0 - 0.95).abs() <= 0.005);
        assert!((tens as f64 / 50_000.0 - 0.04).abs() <= 0.003);
        assert!((hundreds as f64 / 50_000.0 - 0.009).abs() <= 0.002);
        assert!((thousands as f64 / 50_000.0 - 0.001).abs() <= 0.001);
    }

    #[test]
    fn planner_preserves_target_table_ratios_and_schema_ready_rows() {
        let planner = WorkloadPlanner::new(20260901);
        let plans: Vec<_> = (1..=20_000)
            .map(|sequence| planner.plan(sequence))
            .collect();
        let mix = TableMix::from_plans(&plans);
        let insert_total = mix.insert_customers + mix.insert_orders + mix.insert_events;
        let update_total = mix.update_customers + mix.update_orders;
        let delete_total = mix.delete_orders + mix.delete_events;
        assert!((mix.insert_events as f64 / insert_total as f64 - 0.60).abs() <= 0.02);
        assert!((mix.insert_orders as f64 / insert_total as f64 - 0.35).abs() <= 0.02);
        assert!((mix.insert_customers as f64 / insert_total as f64 - 0.05).abs() <= 0.01);
        assert!((mix.update_orders as f64 / update_total as f64 - 0.90).abs() <= 0.02);
        assert!((mix.update_customers as f64 / update_total as f64 - 0.10).abs() <= 0.02);
        assert!((mix.delete_orders as f64 / delete_total as f64 - 0.80).abs() <= 0.02);
        assert!((mix.delete_events as f64 / delete_total as f64 - 0.20).abs() <= 0.02);

        for plan in plans.iter().take(500) {
            for operation in &plan.operations {
                match operation {
                    Operation::InsertCustomer(row) => {
                        assert!(row.customer_id > APP_CUSTOMER_BASE);
                        assert!(row.tenant_id >= 1);
                        assert!(!row.email_domain.is_empty());
                    }
                    Operation::InsertOrder(row) => {
                        assert!(row.order_id > APP_ORDER_BASE);
                        assert!(row.customer_id >= 1);
                        assert!(row.tenant_id >= 1);
                    }
                    Operation::InsertOrderEvent(row) => {
                        assert!(row.event_id > APP_EVENT_BASE);
                        assert!(row.order_id > APP_ORDER_BASE);
                        assert!(row.tenant_id >= 1);
                    }
                    Operation::UpdateCustomer {
                        customer_id,
                        tenant_id,
                        ..
                    } => {
                        assert!(*customer_id > APP_CUSTOMER_BASE);
                        assert!(*tenant_id >= 1);
                    }
                    Operation::UpdateOrder {
                        order_id,
                        tenant_id,
                        customer_id,
                        ..
                    } => {
                        assert!(*order_id > APP_ORDER_BASE);
                        assert!(*tenant_id >= 1);
                        assert!(*customer_id >= 1);
                    }
                    Operation::DeleteOrder {
                        order_id,
                        tenant_id,
                    } => {
                        assert!(*order_id > APP_ORDER_BASE);
                        assert!(*tenant_id >= 1);
                    }
                    Operation::DeleteOrderEvent {
                        event_id,
                        order_id,
                        tenant_id,
                    } => {
                        assert!(*event_id > APP_EVENT_BASE);
                        assert!(*order_id > APP_ORDER_BASE);
                        assert!(*tenant_id >= 1);
                    }
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limiter_counts_each_request_once_per_interval() {
        let mut limiter = RateLimiter::new(10, 10);
        limiter.acquire(10).await.unwrap();
        let second = tokio::spawn(async move {
            let mut limiter = limiter;
            limiter.acquire(10).await.unwrap();
            tokio::time::advance(Duration::from_secs(60)).await;
            limiter.acquire(1).await.unwrap();
            limiter
        });
        tokio::time::advance(Duration::from_secs(1)).await;
        let limiter = second.await.unwrap();
        assert_eq!(
            limiter.intervals(),
            &[RateInterval {
                target_rows: 20,
                achieved_rows: 20
            }]
        );
    }
}
