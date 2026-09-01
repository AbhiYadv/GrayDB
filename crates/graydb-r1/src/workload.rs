use crate::generator::mix64;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomerRow {
    pub customer_id: u64,
    pub segment: String,
    pub profile_json: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRow {
    pub order_id: u64,
    pub status: String,
    pub amount_cents: i64,
    pub updated_at_micros: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderEventRow {
    pub event_id: u64,
    pub event_type: String,
    pub metadata_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    InsertCustomer(CustomerRow),
    InsertOrder(OrderRow),
    InsertOrderEvent(OrderEventRow),
    UpdateCustomer {
        customer_id: u64,
        segment: String,
        profile_json: String,
    },
    UpdateOrder {
        order_id: u64,
        status: String,
        amount_cents: i64,
        updated_at_micros: i64,
    },
    DeleteOrder {
        order_id: u64,
    },
    DeleteOrderEvent {
        event_id: u64,
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

#[derive(Debug, Clone, Copy)]
pub struct WorkloadPlanner {
    pub seed: u64,
}
impl WorkloadPlanner {
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }
    pub fn plan(&self, sequence: u64) -> TransactionPlan {
        let size = transaction_size(self.seed, sequence);
        let offset: u64 = (1..sequence)
            .map(|s| transaction_size(self.seed, s) as u64)
            .sum();
        let mut operations = Vec::with_capacity(size);
        for i in 0..size {
            let n = mix64(self.seed ^ sequence.wrapping_mul(0x9e3779b97f4a7c15) ^ i as u64);
            let id = sequence * 100 + i as u64 + 1;
            // Weighted round-robin keeps the observed operation mix exact over long runs.
            let kind = (offset + i as u64) % 100;
            if kind < 90 {
                match n % 3 {
                    0 => operations.push(Operation::InsertCustomer(CustomerRow {
                        customer_id: id,
                        segment: format!("segment-{}", n % 4),
                        profile_json: format!("{{\"seed\":{n}}}"),
                    })),
                    1 => operations.push(Operation::InsertOrder(OrderRow {
                        order_id: id,
                        status: "pending".into(),
                        amount_cents: (n % 200_000) as i64,
                        updated_at_micros: 1_609_459_200_000_000 + (id as i64),
                    })),
                    _ => operations.push(Operation::InsertOrderEvent(OrderEventRow {
                        event_id: id,
                        event_type: "created".into(),
                        metadata_json: format!("{{\"v\":{}}}", n % 1000),
                    })),
                }
            } else if kind < 98 {
                if n % 2 == 0 {
                    operations.push(Operation::UpdateCustomer {
                        customer_id: id,
                        segment: format!("segment-{}", n % 4),
                        profile_json: format!("{{\"seed\":{n}}}"),
                    });
                } else {
                    operations.push(Operation::UpdateOrder {
                        order_id: id,
                        status: "paid".into(),
                        amount_cents: (n % 200_000) as i64,
                        updated_at_micros: 1_609_459_200_000_000 + (id as i64),
                    });
                }
            } else if n % 2 == 0 {
                operations.push(Operation::DeleteOrder { order_id: id });
            } else {
                operations.push(Operation::DeleteOrderEvent { event_id: id });
            }
        }
        let bytes = serde_json::to_vec(&operations).expect("operation serialization");
        let digest = Sha256::digest(bytes);
        TransactionPlan {
            sequence,
            logical_time_micros: sequence as i64 * 1_000_000,
            operations,
            operation_sha256: hex(&digest),
        }
    }
}
fn transaction_size(seed: u64, sequence: u64) -> usize {
    match mix64(seed ^ sequence.rotate_left(17)) % 10_000 {
        0..=9_499 => 1,
        9_500..=9_899 => 2,
        9_900..=9_989 => 5,
        9_990..=9_999 => 10,
        _ => 20,
    }
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct RowMix {
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
}
impl RowMix {
    pub fn from_plans(p: &[TransactionPlan]) -> Self {
        p.iter()
            .flat_map(|x| x.operations.iter())
            .fold(Self::default(), |mut m, o| {
                if o.is_insert() {
                    m.inserts += 1
                } else if o.is_update() {
                    m.updates += 1
                } else {
                    m.deletes += 1
                };
                m
            })
    }
    fn f(n: u64, total: u64) -> f64 {
        if total == 0 {
            0.0
        } else {
            n as f64 / total as f64
        }
    }
    pub fn insert_fraction(&self) -> f64 {
        Self::f(self.inserts, self.total())
    }
    pub fn update_fraction(&self) -> f64 {
        Self::f(self.updates, self.total())
    }
    pub fn delete_fraction(&self) -> f64 {
        Self::f(self.deletes, self.total())
    }
    fn total(&self) -> u64 {
        self.inserts + self.updates + self.deletes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn planner_is_reproducible_and_converges_on_row_mix() {
        let p = WorkloadPlanner::new(20260901);
        let a: Vec<_> = (1..=20_000).map(|s| p.plan(s)).collect();
        let b: Vec<_> = (1..=20_000).map(|s| p.plan(s)).collect();
        assert_eq!(a, b);
        let m = RowMix::from_plans(&a);
        eprintln!(
            "mix {}/{}/{} = {:.4}/{:.4}/{:.4}",
            m.inserts,
            m.updates,
            m.deletes,
            m.insert_fraction(),
            m.update_fraction(),
            m.delete_fraction()
        );
        assert!((m.insert_fraction() - 0.90).abs() <= 0.005);
        assert!((m.update_fraction() - 0.08).abs() <= 0.005);
        assert!((m.delete_fraction() - 0.02).abs() <= 0.005);
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
    pub intervals: Vec<RateInterval>,
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
        };
        loop {
            let now = tokio::time::Instant::now();
            let elapsed = (now - self.last).as_secs_f64();
            self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
            self.last = now;
            if now - self.interval_start >= Duration::from_secs(60) {
                self.intervals.push(RateInterval {
                    target_rows: self.interval_target,
                    achieved_rows: self.interval_achieved,
                });
                self.interval_target = 0;
                self.interval_achieved = 0;
                self.interval_start = now;
            }
            self.interval_target += rows;
            if self.tokens >= rows as f64 {
                self.tokens -= rows as f64;
                self.interval_achieved += rows;
                return Ok(());
            }
            let wait = (rows as f64 - self.tokens) / self.rate;
            tokio::time::sleep(Duration::from_secs_f64(wait.max(0.000_001))).await;
        }
    }
}
