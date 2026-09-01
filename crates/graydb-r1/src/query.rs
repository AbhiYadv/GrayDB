use crate::contracts::LogicalCheckpoint;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryId {
    Q1,
    Q2,
    Q3,
    Q4,
    Q5,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryParameters {
    pub window_end_micros: i64,
    pub tenant_id: u64,
    pub tenant_set: Vec<u64>,
}
impl QueryParameters {
    pub fn for_checkpoint(seed: u64, c: LogicalCheckpoint) -> Self {
        let window_end_micros = 1_609_459_200_000_000
            + ((crate::generator::mix64(seed ^ c.sequence ^ c.source_lsn) % 31_536_000) * 1_000_000)
                as i64;
        let tenant_id = 1 + crate::generator::mix64(seed ^ c.sequence).wrapping_rem(10_000);
        let start = tenant_id;
        Self {
            window_end_micros,
            tenant_id,
            tenant_set: (0..8)
                .map(|i| 1 + crate::generator::mix64(seed ^ c.sequence + i).wrapping_rem(10_000))
                .collect::<Vec<_>>()
                .into_iter()
                .chain([start])
                .take(8)
                .collect(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuerySchedule {
    pub ordinal: u64,
    pub checkpoint: LogicalCheckpoint,
    pub query: QueryId,
    pub parameters: QueryParameters,
}
impl QuerySchedule {
    pub fn new(seed: u64) -> ScheduleBuilder {
        ScheduleBuilder { seed }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct ScheduleBuilder {
    seed: u64,
}
impl ScheduleBuilder {
    pub fn at(&self, ordinal: u64, checkpoint: LogicalCheckpoint) -> QuerySchedule {
        let q = match crate::generator::mix64(self.seed ^ ordinal) % 5 {
            0 => QueryId::Q1,
            1 => QueryId::Q2,
            2 => QueryId::Q3,
            3 => QueryId::Q4,
            _ => QueryId::Q5,
        };
        QuerySchedule {
            ordinal,
            checkpoint,
            query: q,
            parameters: QueryParameters::for_checkpoint(self.seed, checkpoint),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}
pub fn canonical_digest(result: &QueryResult) -> String {
    let mut encoded = Vec::new();
    for column in &result.columns {
        encoded.extend_from_slice(column.len().to_string().as_bytes());
        encoded.push(b':');
        encoded.extend_from_slice(column.as_bytes());
        encoded.push(b';');
    }
    let mut rows: Vec<Vec<u8>> = result
        .rows
        .iter()
        .map(|row| {
            let mut out = Vec::new();
            for v in row {
                match v {
                    None => out.extend_from_slice(b"N;"),
                    Some(s) => {
                        out.extend_from_slice(s.len().to_string().as_bytes());
                        out.push(b':');
                        out.extend_from_slice(s.as_bytes());
                        out.push(b';');
                    }
                }
            }
            out
        })
        .collect();
    rows.sort();
    for row in rows {
        encoded.extend(row);
    }
    let mut h = Sha256::new();
    h.update(encoded);
    encode_hex(&h.finalize())
}
fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub fn render_sql(sql: &str, p: &QueryParameters) -> Result<String, String> {
    let tenant_set = p
        .tenant_set
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let rendered = sql
        .replace(
            ":window_end",
            &format!("to_timestamp({})", p.window_end_micros as f64 / 1_000_000.0),
        )
        .replace(":tenant_id", &p.tenant_id.to_string())
        .replace(":tenant_set", &tenant_set);
    if rendered.contains(':') {
        return Err("unresolved named parameter".into());
    }
    Ok(rendered)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn schedule_and_parameters_ignore_wall_clock() {
        let c = LogicalCheckpoint {
            sequence: 42_000,
            source_lsn: 0xA000_1234,
        };
        let a = QuerySchedule::new(20260901).at(17, c);
        let b = QuerySchedule::new(20260901).at(17, c);
        assert_eq!(a, b);
        assert_eq!(a.parameters, QueryParameters::for_checkpoint(20260901, c));
    }
    #[test]
    fn digest_ignores_row_order_but_not_boundaries() {
        let a = QueryResult {
            columns: vec!["x".into()],
            rows: vec![vec![Some("a".into())], vec![Some("bc".into())]],
        };
        let b = QueryResult {
            columns: a.columns.clone(),
            rows: vec![vec![Some("bc".into())], vec![Some("a".into())]],
        };
        assert_eq!(canonical_digest(&a), canonical_digest(&b));
        let c = QueryResult {
            columns: a.columns.clone(),
            rows: vec![vec![Some("ab".into())], vec![Some("c".into())]],
        };
        assert_ne!(canonical_digest(&a), canonical_digest(&c));
    }
}
