//! WAL retention budget gauge (WL5 / Amendment A A3).
//! Retained = pg_current_wal_lsn() - slot restart_lsn: the WAL the source must keep
//! for us. Rungs by consumed fraction: 1 warn (>= warn_fraction), 2 shed
//! (>= shed_fraction). Rung 3 (spill to staging) is condition-triggered — write
//! path degraded — and is reported by the pump/log, not by this gauge.

use crate::repl::parse_lsn;
use anyhow::{Context, Result};
use tokio_postgres::Client;

#[derive(Debug, Clone, Copy)]
pub struct BudgetSnapshot {
    pub head: u64,
    pub restart_lsn: u64,
    pub confirmed_flush: u64,
    pub retained_bytes: u64,
    pub consumed_fraction: f64,
    /// 0 = healthy, 1 = warn, 2 = shed (per fractions in graydb.toml).
    pub rung: u8,
}

pub async fn sample(
    client: &Client,
    slot: &str,
    budget_bytes: u64,
    warn_fraction: f64,
    shed_fraction: f64,
) -> Result<BudgetSnapshot> {
    let row = client
        .query_one(
            "SELECT pg_current_wal_lsn()::text,
                    restart_lsn::text,
                    confirmed_flush_lsn::text
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .context("sampling slot for WAL budget")?;
    let head = parse_lsn(row.get::<_, String>(0).as_str())?;
    let restart_lsn = parse_lsn(row.get::<_, String>(1).as_str())?;
    let confirmed_flush = parse_lsn(row.get::<_, String>(2).as_str())?;
    let retained_bytes = head.saturating_sub(restart_lsn);
    let consumed_fraction = retained_bytes as f64 / budget_bytes.max(1) as f64;
    let rung = if consumed_fraction >= shed_fraction {
        2
    } else if consumed_fraction >= warn_fraction {
        1
    } else {
        0
    };
    Ok(BudgetSnapshot {
        head,
        restart_lsn,
        confirmed_flush,
        retained_bytes,
        consumed_fraction,
        rung,
    })
}
