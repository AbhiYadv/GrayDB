use anyhow::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ops::Range;

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
    pub fn row(&self, table: Table, id: u64) -> Row {
        let tenant = 1 + self.draw(table, id, 1) % 10_000;
        let created = 1_609_459_200_i64 + (self.draw(table, id, 2) % 31_536_000) as i64;
        let ts = format_timestamp(created);
        match table {
            Table::Tenants => Row::Tenants {
                tenant_id: id,
                region: ["us-east", "us-west", "eu-west", "ap-south"]
                    [(self.draw(table, id, 3) % 4) as usize]
                    .into(),
                plan: ["free", "pro", "enterprise"][(self.draw(table, id, 4) % 3) as usize].into(),
                created_at: ts,
                settings: json(self.draw(table, id, 5), "tenant"),
            },
            Table::Customers => Row::Customers {
                customer_id: id,
                tenant_id: tenant,
                segment: ["consumer", "smb", "mid-market", "enterprise"]
                    [(self.draw(table, id, 3) % 4) as usize]
                    .into(),
                email_domain: ["example.com", "mail.test", "corp.test", "acme.test"]
                    [(self.draw(table, id, 4) % 4) as usize]
                    .into(),
                profile: json(self.draw(table, id, 5), "customer"),
                created_at: ts,
            },
            Table::Orders => {
                let updated = format_timestamp(created + (self.draw(table, id, 6) % 86_400) as i64);
                Row::Orders {
                    order_id: id,
                    tenant_id: tenant,
                    customer_id: 1 + self.draw(table, id, 7) % 10_000_000,
                    status: ["pending", "paid", "shipped", "cancelled", "refunded"]
                        [(self.draw(table, id, 3) % 5) as usize]
                        .into(),
                    channel: ["web", "mobile", "partner"][(self.draw(table, id, 4) % 3) as usize]
                        .into(),
                    amount_cents: 100 + (self.draw(table, id, 5) % 999_900) as i64,
                    created_at: ts,
                    updated_at: updated,
                    attributes: json(self.draw(table, id, 8), "order"),
                }
            }
            Table::OrderEvents => Row::OrderEvents {
                event_id: id,
                order_id: 1 + self.draw(table, id, 3) % 10_000_000,
                tenant_id: tenant,
                event_type: ["created", "paid", "fulfilled", "cancelled", "returned"]
                    [(self.draw(table, id, 4) % 5) as usize]
                    .into(),
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
fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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
    format!(r#"{{"{}":{},"version":1}}"#, k, v % 1000)
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
}
