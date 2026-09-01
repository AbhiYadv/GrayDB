use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ops::Range;

pub const COPY_BATCH_ROWS: u64 = 100_000;
const MAX_TENANT_ACTIVITY_RANK: u64 = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRange {
    pub table: Table,
    pub range: Range<u64>,
}

/// Stable initial-load cycle: each complete cycle has 1 tenant, 5 customers,
/// 20 orders, and 60 events. Truncating the returned vector is prefix-stable.
pub fn cycle_ranges(cycles: u64) -> Vec<TableRange> {
    let mut out = Vec::with_capacity(cycles as usize * 4);
    for cycle in 0..cycles {
        let base = cycle * 100;
        for (table, count) in [
            (Table::Tenants, 1),
            (Table::Customers, 5),
            (Table::Orders, 20),
            (Table::OrderEvents, 60),
        ] {
            out.push(TableRange {
                table,
                range: base + 1..base + count + 1,
            });
        }
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Table {
    Tenants,
    Customers,
    Orders,
    OrderEvents,
}
impl Table {
    pub fn tag(self) -> u64 {
        match self {
            Self::Tenants => 1,
            Self::Customers => 2,
            Self::Orders => 3,
            Self::OrderEvents => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Row {
    Tenants {
        tenant_id: u64,
        region: String,
        plan: String,
        created_at: String,
        settings: String,
    },
    Customers {
        customer_id: u64,
        tenant_id: u64,
        segment: String,
        email_domain: String,
        profile: String,
        created_at: String,
    },
    Orders {
        order_id: u64,
        tenant_id: u64,
        customer_id: u64,
        status: String,
        channel: String,
        amount_cents: i64,
        created_at: String,
        updated_at: String,
        attributes: String,
    },
    OrderEvents {
        event_id: u64,
        order_id: u64,
        tenant_id: u64,
        event_type: String,
        event_at: String,
        metadata: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopyBatch {
    pub table: Table,
    pub rows: u64,
    pub bytes: Vec<u8>,
    pub sha256: String,
}

#[derive(Debug, Clone, Copy)]
pub struct DeterministicGenerator {
    pub seed: u64,
}
impl DeterministicGenerator {
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }
    pub fn copy_batches(&self, table: Table, range: Range<u64>) -> Result<Vec<CopyBatch>> {
        let mut out = Vec::new();
        let mut start = range.start;
        while start < range.end {
            let end = (start + COPY_BATCH_ROWS).min(range.end);
            out.push(self.copy_batch(table, start..end)?);
            start = end;
        }
        Ok(out)
    }
    pub fn row(&self, table: Table, id: u64) -> Row {
        let selected_order = order_for_event(id, self.draw(table, id, 3));
        let selected_customer = match table {
            Table::OrderEvents => {
                customer_for_order(selected_order, self.draw(Table::Orders, selected_order, 7))
            }
            _ => customer_for_order(id, self.draw(Table::Orders, id, 7)),
        };
        let tenant = match table {
            Table::Tenants => id,
            Table::Customers => tenant_for_customer(id, self.draw(Table::Customers, id, 11)),
            Table::Orders | Table::OrderEvents => tenant_for_customer(
                selected_customer,
                self.draw(Table::Customers, selected_customer, 11),
            ),
        };
        // Squaring the bounded draw concentrates records toward the recent end.
        let age = self.draw(table, id, 2) % 31_536_000;
        let created = 1_609_459_200_i64 + 31_536_000_i64 - ((age * age) / 31_536_000) as i64;
        let ts = format_timestamp(created);
        match table {
            Table::Tenants => Row::Tenants {
                tenant_id: id,
                region: weighted_choice(self.draw(table, id, 3), &REGIONS).into(),
                plan: weighted_choice(self.draw(table, id, 4), &PLANS).into(),
                created_at: ts,
                settings: json(self.draw(table, id, 5), "tenant"),
            },
            Table::Customers => Row::Customers {
                customer_id: id,
                tenant_id: tenant,
                segment: weighted_choice(self.draw(table, id, 3), &SEGMENTS).into(),
                email_domain: weighted_choice(self.draw(table, id, 4), &EMAIL_DOMAINS).into(),
                profile: json(self.draw(table, id, 5), "customer"),
                created_at: ts,
            },
            Table::Orders => {
                let updated = format_timestamp(created + (self.draw(table, id, 6) % 86_400) as i64);
                Row::Orders {
                    order_id: id,
                    tenant_id: tenant,
                    customer_id: selected_customer,
                    status: weighted_choice(self.draw(table, id, 3), &STATUSES).into(),
                    channel: weighted_choice(self.draw(table, id, 4), &CHANNELS).into(),
                    amount_cents: long_tailed_cents(
                        self.draw(table, id, 5),
                        self.draw(table, id, 9),
                    ),
                    created_at: ts,
                    updated_at: updated,
                    attributes: json(self.draw(table, id, 8), "order"),
                }
            }
            Table::OrderEvents => Row::OrderEvents {
                event_id: id,
                order_id: selected_order,
                tenant_id: tenant,
                event_type: weighted_choice(self.draw(table, id, 4), &EVENT_TYPES).into(),
                event_at: ts,
                metadata: json(self.draw(table, id, 5), "event"),
            },
        }
    }
    pub fn copy_batch(&self, table: Table, range: Range<u64>) -> Result<CopyBatch> {
        let mut bytes = Vec::new();
        let mut rows = 0;
        for id in range {
            let fields = self.fields(self.row(table, id));
            for (i, f) in fields.iter().enumerate() {
                if i > 0 {
                    bytes.push(b'\t');
                }
                bytes.extend_from_slice(escape(f).as_bytes());
            }
            bytes.push(b'\n');
            rows += 1;
        }
        let mut h = Sha256::new();
        h.update(&bytes);
        Ok(CopyBatch {
            table,
            rows,
            sha256: encode_hex(&h.finalize()),
            bytes,
        })
    }
    fn draw(&self, t: Table, id: u64, salt: u64) -> u64 {
        mix64(self.seed ^ t.tag() ^ id.rotate_left(17) ^ salt.rotate_left(41))
    }
    fn fields(&self, row: Row) -> Vec<String> {
        match row {
            Row::Tenants {
                tenant_id,
                region,
                plan,
                created_at,
                settings,
            } => vec![tenant_id.to_string(), region, plan, created_at, settings],
            Row::Customers {
                customer_id,
                tenant_id,
                segment,
                email_domain,
                profile,
                created_at,
            } => vec![
                customer_id.to_string(),
                tenant_id.to_string(),
                segment,
                email_domain,
                profile,
                created_at,
            ],
            Row::Orders {
                order_id,
                tenant_id,
                customer_id,
                status,
                channel,
                amount_cents,
                created_at,
                updated_at,
                attributes,
            } => vec![
                order_id.to_string(),
                tenant_id.to_string(),
                customer_id.to_string(),
                status,
                channel,
                amount_cents.to_string(),
                created_at,
                updated_at,
                attributes,
            ],
            Row::OrderEvents {
                event_id,
                order_id,
                tenant_id,
                event_type,
                event_at,
                metadata,
            } => vec![
                event_id.to_string(),
                order_id.to_string(),
                tenant_id.to_string(),
                event_type,
                event_at,
                metadata,
            ],
        }
    }
}
fn tenant_for_customer(id: u64, draw: u64) -> u64 {
    let loaded_tenants = cycle_base(id) / 100 + 1;
    let rank = bounded_zipf_rank(draw, loaded_tenants.min(MAX_TENANT_ACTIVITY_RANK));
    (rank - 1) * 100 + 1
}
fn customer_for_order(order_id: u64, draw: u64) -> u64 {
    cycle_base(order_id) + 1 + draw % 5
}
fn cycle_base(id: u64) -> u64 {
    id.saturating_sub(1) / 100 * 100
}
fn order_for_event(id: u64, draw: u64) -> u64 {
    cycle_base(id) + 1 + draw % 20
}
fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
const REGIONS: [(&str, u64); 4] = [
    ("us-east", 46),
    ("us-west", 27),
    ("eu-west", 18),
    ("ap-south", 9),
];
const PLANS: [(&str, u64); 3] = [("free", 70), ("pro", 23), ("enterprise", 7)];
const SEGMENTS: [(&str, u64); 4] = [
    ("consumer", 55),
    ("smb", 29),
    ("mid-market", 12),
    ("enterprise", 4),
];
const EMAIL_DOMAINS: [(&str, u64); 4] = [
    ("example.com", 58),
    ("mail.test", 25),
    ("corp.test", 12),
    ("acme.test", 5),
];
const STATUSES: [(&str, u64); 5] = [
    ("paid", 46),
    ("shipped", 27),
    ("pending", 15),
    ("cancelled", 8),
    ("refunded", 4),
];
const CHANNELS: [(&str, u64); 3] = [("web", 61), ("mobile", 31), ("partner", 8)];
const EVENT_TYPES: [(&str, u64); 5] = [
    ("created", 42),
    ("paid", 30),
    ("fulfilled", 17),
    ("cancelled", 7),
    ("returned", 4),
];

fn weighted_choice<'a, const N: usize>(draw: u64, choices: &'a [(&'a str, u64); N]) -> &'a str {
    let total = choices.iter().map(|(_, weight)| weight).sum::<u64>();
    let mut slot = draw % total;
    for (value, weight) in choices {
        if slot < *weight {
            return value;
        }
        slot -= weight;
    }
    unreachable!("non-empty weighted dictionary")
}

fn long_tailed_cents(bucket_draw: u64, value_draw: u64) -> i64 {
    const BUCKETS: [(i64, i64, u64); 5] = [
        (100, 2_499, 55),
        (2_500, 9_999, 27),
        (10_000, 49_999, 12),
        (50_000, 249_999, 5),
        (250_000, 2_000_000, 1),
    ];
    let mut slot = bucket_draw % 100;
    for (low, high, weight) in BUCKETS {
        if slot < weight {
            return low + (value_draw % (high - low + 1) as u64) as i64;
        }
        slot -= weight;
    }
    unreachable!("long-tail buckets sum to 100")
}
pub fn mix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}
fn escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}
fn json(v: u64, k: &str) -> String {
    let mut tags = BTreeMap::new();
    tags.insert("bucket".to_string(), Value::from((v % 8) as u64));
    tags.insert(
        "label".to_string(),
        Value::from("x".repeat(json_label_len(v))),
    );
    let mut object = BTreeMap::new();
    if k == "tenant" {
        object.insert(
            "activity_rank".to_string(),
            Value::from(bounded_zipf_rank(v, MAX_TENANT_ACTIVITY_RANK)),
        );
    }
    object.insert("kind".to_string(), Value::from(k));
    object.insert("payload".to_string(), Value::from(v % 1000));
    object.insert("schema_version".to_string(), Value::from(1));
    object.insert(
        "tags".to_string(),
        serde_json::to_value(tags).expect("JSON tags"),
    );
    serde_json::to_string(&object).expect("ordered JSON metadata")
}
fn bounded_zipf_rank(draw: u64, max_rank: u64) -> u64 {
    debug_assert!(max_rank > 0 && max_rank <= MAX_TENANT_ACTIVITY_RANK);
    let total = (1..=max_rank).map(|rank| 10_000 / rank).sum::<u64>();
    let mut slot = draw % total;
    for rank in 1..=max_rank {
        let weight = 10_000 / rank;
        if slot < weight {
            return rank;
        }
        slot -= weight;
    }
    unreachable!("bounded Zipf weights are non-empty")
}
fn json_label_len(v: u64) -> usize {
    match v % 100 {
        0..=54 => 3,
        55..=81 => 16,
        82..=93 => 64,
        _ => 128,
    }
}
fn format_timestamp(seconds: i64) -> String {
    format!(
        "{}-{:02}-{:02} 00:00:00+00",
        1970 + seconds / 31_536_000,
        1 + (seconds / 2_592_000) % 12,
        1 + (seconds / 86_400) % 28
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    #[test]
    fn same_seed_and_range_produce_identical_copy_bytes() {
        let a = DeterministicGenerator::new(20260901)
            .copy_batch(Table::Orders, 1..10_001)
            .unwrap();
        let b = DeterministicGenerator::new(20260901)
            .copy_batch(Table::Orders, 1..10_001)
            .unwrap();
        assert_eq!(a.bytes, b.bytes);
        assert_eq!(a.sha256, b.sha256);
        assert_eq!(a.rows, 10_000);
    }
    #[test]
    fn row_ids_are_stable() {
        assert_eq!(
            DeterministicGenerator::new(1).row(Table::Tenants, 4),
            DeterministicGenerator::new(1).row(Table::Tenants, 4)
        );
    }
    #[test]
    fn relationships_are_same_tenant_and_prefix_stable() {
        let g = DeterministicGenerator::new(20260901);
        for id in 1..1000 {
            let Row::Orders {
                tenant_id,
                customer_id,
                ..
            } = g.row(Table::Orders, id)
            else {
                unreachable!()
            };
            let Row::Customers {
                tenant_id: customer_tenant,
                ..
            } = g.row(Table::Customers, customer_id)
            else {
                unreachable!()
            };
            assert_eq!(tenant_id, customer_tenant);
            let Row::OrderEvents {
                order_id,
                tenant_id: event_tenant,
                ..
            } = g.row(Table::OrderEvents, id)
            else {
                unreachable!()
            };
            let Row::Orders {
                tenant_id: order_tenant,
                ..
            } = g.row(Table::Orders, order_id)
            else {
                unreachable!()
            };
            assert_eq!(event_tenant, order_tenant);
        }
        let short = g.copy_batch(Table::Orders, 1..20).unwrap();
        let long = g.copy_batch(Table::Orders, 1..40).unwrap();
        assert!(long.bytes.starts_with(&short.bytes));
    }
    #[test]
    fn cycle_allocator_is_deterministic() {
        assert_eq!(cycle_ranges(3), cycle_ranges(3));
        assert_eq!(
            cycle_ranges(1)
                .iter()
                .map(|r| r.range.end - r.range.start)
                .sum::<u64>(),
            86
        );
        let g = DeterministicGenerator::new(20260901);
        let ranges = cycle_ranges(1);
        let ids = |table: Table| {
            ranges
                .iter()
                .find(|r| r.table == table)
                .unwrap()
                .range
                .clone()
                .collect::<std::collections::BTreeSet<_>>()
        };
        let customers = ids(Table::Customers);
        let orders = ids(Table::Orders);
        let tenants = ids(Table::Tenants);
        for id in 1..61 {
            match g.row(Table::OrderEvents, id) {
                Row::OrderEvents {
                    order_id,
                    tenant_id,
                    ..
                } => {
                    assert!(orders.contains(&order_id));
                    assert!(tenants.contains(&tenant_id));
                }
                _ => unreachable!(),
            }
        }
        for id in 1..21 {
            match g.row(Table::Orders, id) {
                Row::Orders {
                    customer_id,
                    tenant_id,
                    ..
                } => {
                    assert!(customers.contains(&customer_id));
                    assert!(tenants.contains(&tenant_id));
                }
                _ => unreachable!(),
            }
        }
    }
    #[test]
    fn first_cycle_orders_and_events_reference_loaded_prefix_rows() {
        let g = DeterministicGenerator::new(20260901);
        for id in 1..=20 {
            let Row::Orders {
                tenant_id,
                customer_id,
                ..
            } = g.row(Table::Orders, id)
            else {
                unreachable!()
            };
            assert_eq!(tenant_id, 1);
            assert!((1..=5).contains(&customer_id));
        }
        for id in 1..=60 {
            let Row::OrderEvents {
                tenant_id,
                order_id,
                ..
            } = g.row(Table::OrderEvents, id)
            else {
                unreachable!()
            };
            assert_eq!(tenant_id, 1);
            assert!((1..=20).contains(&order_id));
        }
    }
    #[test]
    fn categorical_dictionaries_are_deterministically_skewed() {
        let g = DeterministicGenerator::new(20260901);
        let mut plans = BTreeMap::new();
        let mut statuses = BTreeMap::new();
        for id in 1..=10_000 {
            let Row::Tenants { plan, .. } = g.row(Table::Tenants, id) else {
                unreachable!()
            };
            *plans.entry(plan).or_insert(0_u64) += 1;
            let Row::Orders { status, .. } = g.row(Table::Orders, id) else {
                unreachable!()
            };
            *statuses.entry(status).or_insert(0_u64) += 1;
        }
        assert!(plans["free"] > plans["pro"] && plans["pro"] > plans["enterprise"]);
        assert!(
            statuses["paid"] > statuses["shipped"] && statuses["shipped"] > statuses["refunded"]
        );
    }
    #[test]
    fn json_is_btree_ordered_structured_and_size_bounded() {
        let value = json(123, "order");
        assert_eq!(
            value,
            r#"{"kind":"order","payload":123,"schema_version":1,"tags":{"bucket":3,"label":"xxx"}}"#
        );
        let g = DeterministicGenerator::new(20260901);
        let mut sizes = BTreeMap::new();
        for id in 1..=128 {
            let Row::OrderEvents { metadata, .. } = g.row(Table::OrderEvents, id) else {
                unreachable!()
            };
            assert!(metadata.len() <= 256);
            *sizes.entry(metadata.len()).or_insert(0_u64) += 1;
        }
        assert!(
            sizes.len() >= 3,
            "JSON size distribution collapsed: {sizes:?}"
        );
    }
    #[test]
    fn tenant_activity_rank_uses_a_bounded_zipf_shape() {
        let g = DeterministicGenerator::new(20260901);
        let mut ranks = BTreeMap::new();
        for id in 1..=10_000 {
            let Row::Tenants { settings, .. } = g.row(Table::Tenants, id) else {
                unreachable!()
            };
            let rank = serde_json::from_str::<Value>(&settings).unwrap()["activity_rank"]
                .as_u64()
                .unwrap();
            assert!((1..=64).contains(&rank));
            *ranks.entry(rank).or_insert(0_u64) += 1;
        }
        assert!(ranks[&1] > ranks[&2]);
        assert!(ranks.len() > 8);
    }

    #[test]
    fn generated_ownership_is_materially_skewed_across_complete_cycles() {
        let g = DeterministicGenerator::new(20260901);
        let mut customer_counts = BTreeMap::new();
        let mut order_counts = BTreeMap::new();
        let mut event_counts = BTreeMap::new();

        for table_range in cycle_ranges(512) {
            for id in table_range.range {
                let (counts, tenant_id) = match g.row(table_range.table, id) {
                    Row::Customers { tenant_id, .. } => (&mut customer_counts, tenant_id),
                    Row::Orders { tenant_id, .. } => (&mut order_counts, tenant_id),
                    Row::OrderEvents { tenant_id, .. } => (&mut event_counts, tenant_id),
                    Row::Tenants { .. } => continue,
                };
                *counts.entry(tenant_id).or_insert(0_u64) += 1;
            }
        }

        let hot_tenant = 1;
        let cold_tenant = 1_501;
        for (kind, counts) in [
            ("customers", customer_counts),
            ("orders", order_counts),
            ("events", event_counts),
        ] {
            let hot = counts.get(&hot_tenant).copied().unwrap_or_default();
            let cold = counts.get(&cold_tenant).copied().unwrap_or_default();
            assert!(cold > 0, "cold tenant has no generated {kind}");
            assert!(
                hot > cold * 5,
                "expected materially skewed {kind} ownership: hot={hot}, cold={cold}"
            );
        }
    }

    #[test]
    fn referenced_tenants_are_present_in_the_loaded_cycle_prefix() {
        let g = DeterministicGenerator::new(20260901);
        let mut loaded_tenants = std::collections::BTreeSet::new();

        for table_range in cycle_ranges(256) {
            for id in table_range.range {
                match g.row(table_range.table, id) {
                    Row::Tenants { tenant_id, .. } => {
                        loaded_tenants.insert(tenant_id);
                    }
                    Row::Customers { tenant_id, .. }
                    | Row::Orders { tenant_id, .. }
                    | Row::OrderEvents { tenant_id, .. } => assert!(
                        loaded_tenants.contains(&tenant_id),
                        "tenant {tenant_id} was referenced before its cycle was loaded"
                    ),
                }
            }
        }
    }
}
