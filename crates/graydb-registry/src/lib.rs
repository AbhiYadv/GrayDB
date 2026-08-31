//! graydb-registry: LSN-versioned schema registry.
//! Consumes in-stream ddl_log rows + Relation messages; answers "schema for table T at LSN L".
//! Drives typed decode and re-materialization classification (matrix classes A-D).
//!
//! Everything here is DERIVED by replaying the frame log (I2/I3: the log is the spine;
//! the registry is disposable and rebuilt from it). Version boundaries are commit
//! LSNs: a Relation message observed inside a transaction takes effect AT that
//! transaction's end_lsn — its own rows already decode under the new shape, earlier
//! commits keep the old one. That is the per-LSN interpretation Demo 6 must prove.

pub mod decoder;
pub mod pgoutput;

use anyhow::{Context, Result};
use pgoutput::{Message, RelationDesc, TupleValue};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaVersion {
    /// Commit end_lsn of the transaction that carried the Relation message.
    pub valid_from_lsn: u64,
    pub replident: u8,
    pub columns: Vec<pgoutput::ColumnDesc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct TableEntry {
    pub qualified_name: String,
    /// Ascending by valid_from_lsn.
    pub versions: Vec<SchemaVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Registry {
    /// Keyed by relation OID (identity survives renames — matrix #4/#11).
    pub tables: BTreeMap<u32, TableEntry>,
}

impl Registry {
    fn record_version(&mut self, rel: &RelationDesc, valid_from_lsn: u64) {
        let entry = self.tables.entry(rel.relid).or_default();
        entry.qualified_name = rel.qualified_name();
        let version = SchemaVersion {
            valid_from_lsn,
            replident: rel.replident,
            columns: rel.columns.clone(),
        };
        // Re-sent Relation messages (fresh session, no shape change) are idempotent.
        if entry.versions.last().map(|v| &v.columns) == Some(&version.columns)
            && entry.versions.last().map(|v| v.replident) == Some(version.replident)
        {
            return;
        }
        entry.versions.push(version);
    }

    /// Schema in force for `relid` at commit LSN `lsn` (inclusive).
    pub fn schema_at(&self, relid: u32, lsn: u64) -> Option<&SchemaVersion> {
        self.tables
            .get(&relid)?
            .versions
            .iter()
            .rev()
            .find(|v| v.valid_from_lsn <= lsn)
    }

    /// Same, by qualified table name.
    pub fn schema_for_table(&self, qualified: &str, lsn: u64) -> Option<&SchemaVersion> {
        let (relid, _) = self
            .tables
            .iter()
            .find(|(_, t)| t.qualified_name == qualified)?;
        self.schema_at(*relid, lsn)
    }

    pub fn persist(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_vec_pretty(self)?)
            .with_context(|| format!("persisting registry to {}", path.display()))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Op {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// One decoded change, named under the schema in force at its position in the stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TypedChange {
    pub commit_lsn: u64,
    pub xid: u32,
    pub table: String,
    pub op: Op,
    /// New image (Insert/Update): (column name, value).
    pub new: Option<Vec<(String, TupleValue)>>,
    /// Old image or key (Update with identity change / Delete).
    pub old: Option<Vec<(String, TupleValue)>>,
}

/// A DDL captured in-stream via graydb.ddl_log (wedge spec section 4, layer 1).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DdlEvent {
    pub commit_lsn: u64,
    pub kind: String,
    pub command_tag: Option<String>,
    pub object_identity: Option<String>,
    pub ddl_text: Option<String>,
}

#[derive(Debug, Default)]
pub struct Replay {
    pub registry: Registry,
    pub changes: Vec<TypedChange>,
    pub ddl_events: Vec<DdlEvent>,
    pub txns: u64,
    pub frames: u64,
}

/// Replay the durable frame log into typed changes + registry + in-stream DDL.
/// Deterministic by construction: same frames -> same result (Demo 7 leans on this).
pub fn replay_log(log_dir: &Path) -> Result<Replay> {
    let mut frames: Vec<graydb_log::Frame> = Vec::new();
    let verification = graydb_log::verify_log(log_dir, false, |f| frames.push(f.clone()))?;
    anyhow::ensure!(
        verification.seq_contiguous && verification.lsn_monotone,
        "frame log failed verification before replay"
    );

    let mut out = Replay::default();
    out.frames = verification.frames;

    // Live decode state: latest Relation per relid (in stream order).
    let mut live_rel: BTreeMap<u32, RelationDesc> = BTreeMap::new();

    // Per-transaction buffers, flushed at Commit with the txn's end_lsn.
    struct TxnBuf {
        xid: u32,
        changes: Vec<TypedChange>,
        relations: Vec<RelationDesc>,
        ddl_rows: Vec<DdlEvent>,
    }
    let mut txn: Option<TxnBuf> = None;

    for frame in &frames {
        let msg = pgoutput::parse(&frame.payload)
            .with_context(|| format!("decoding frame seq {}", frame.seq))?;
        match msg {
            Message::Begin { xid, .. } => {
                txn = Some(TxnBuf {
                    xid,
                    changes: Vec::new(),
                    relations: Vec::new(),
                    ddl_rows: Vec::new(),
                });
            }
            Message::Relation(rel) => {
                if let Some(t) = txn.as_mut() {
                    t.relations.push(rel.clone());
                } else {
                    // Relation outside a txn (fresh session preamble): version takes
                    // effect from this frame's position onward.
                    out.registry.record_version(&rel, frame.lsn_end);
                }
                live_rel.insert(rel.relid, rel);
            }
            Message::Insert { relid, new } => {
                let t = txn.as_mut().context("Insert outside transaction")?;
                let rel = live_rel.get(&relid).context("Insert before Relation")?;
                let named = name_tuple(rel, &new);
                if rel.qualified_name() == "graydb.ddl_log" {
                    t.ddl_rows.push(ddl_event_from_row(&named));
                }
                t.changes.push(TypedChange {
                    commit_lsn: 0, // assigned at commit
                    xid: t.xid,
                    table: rel.qualified_name(),
                    op: Op::Insert,
                    new: Some(named),
                    old: None,
                });
            }
            Message::Update { relid, old, new } => {
                let t = txn.as_mut().context("Update outside transaction")?;
                let rel = live_rel.get(&relid).context("Update before Relation")?;
                t.changes.push(TypedChange {
                    commit_lsn: 0,
                    xid: t.xid,
                    table: rel.qualified_name(),
                    op: Op::Update,
                    new: Some(name_tuple(rel, &new)),
                    old: old.map(|(_, tv)| name_tuple(rel, &tv)),
                });
            }
            Message::Delete { relid, old } => {
                let t = txn.as_mut().context("Delete outside transaction")?;
                let rel = live_rel.get(&relid).context("Delete before Relation")?;
                t.changes.push(TypedChange {
                    commit_lsn: 0,
                    xid: t.xid,
                    table: rel.qualified_name(),
                    op: Op::Delete,
                    new: None,
                    old: Some(name_tuple(rel, &old.1)),
                });
            }
            Message::Truncate { relids, .. } => {
                let t = txn.as_mut().context("Truncate outside transaction")?;
                for relid in relids {
                    if let Some(rel) = live_rel.get(&relid) {
                        t.changes.push(TypedChange {
                            commit_lsn: 0,
                            xid: t.xid,
                            table: rel.qualified_name(),
                            op: Op::Truncate,
                            new: None,
                            old: None,
                        });
                    }
                }
            }
            Message::Commit { end_lsn, .. } => {
                let mut t = txn.take().context("Commit without Begin")?;
                for rel in &t.relations {
                    out.registry.record_version(rel, end_lsn);
                }
                for c in &mut t.changes {
                    c.commit_lsn = end_lsn;
                }
                for d in &mut t.ddl_rows {
                    d.commit_lsn = end_lsn;
                }
                out.changes.append(&mut t.changes);
                out.ddl_events.append(&mut t.ddl_rows);
                out.txns += 1;
            }
            Message::Origin { .. } | Message::Type { .. } | Message::LogicalMessage { .. } => {}
        }
    }
    Ok(out)
}

pub(crate) fn name_tuple(rel: &RelationDesc, values: &[TupleValue]) -> Vec<(String, TupleValue)> {
    rel.columns
        .iter()
        .zip(values.iter())
        .map(|(c, v)| (c.name.clone(), v.clone()))
        .collect()
}

fn text_of<'a>(named: &'a [(String, TupleValue)], col: &str) -> Option<&'a str> {
    named.iter().find_map(|(n, v)| {
        if n == col {
            match v {
                TupleValue::Text(s) => Some(s.as_str()),
                _ => None,
            }
        } else {
            None
        }
    })
}

pub(crate) fn ddl_event_from_row(named: &[(String, TupleValue)]) -> DdlEvent {
    DdlEvent {
        commit_lsn: 0,
        kind: text_of(named, "kind").unwrap_or("?").to_string(),
        command_tag: text_of(named, "command_tag").map(str::to_string),
        object_identity: text_of(named, "object_identity").map(str::to_string),
        ddl_text: text_of(named, "ddl_text").map(str::to_string),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgoutput::ColumnDesc;

    fn rel(relid: u32, cols: &[&str]) -> RelationDesc {
        RelationDesc {
            relid,
            namespace: "app".into(),
            name: "t".into(),
            replident: b'd',
            columns: cols
                .iter()
                .map(|c| ColumnDesc {
                    name: c.to_string(),
                    type_oid: 25,
                    typmod: -1,
                    is_key: false,
                })
                .collect(),
        }
    }

    #[test]
    fn schema_at_picks_version_by_lsn() {
        let mut reg = Registry::default();
        reg.record_version(&rel(1, &["a", "b"]), 100);
        reg.record_version(&rel(1, &["a", "b", "c"]), 200);
        reg.record_version(&rel(1, &["a", "b"]), 300);
        assert_eq!(reg.schema_at(1, 99), None);
        assert_eq!(reg.schema_at(1, 100).unwrap().columns.len(), 2);
        assert_eq!(reg.schema_at(1, 150).unwrap().columns.len(), 2);
        assert_eq!(reg.schema_at(1, 200).unwrap().columns.len(), 3);
        assert_eq!(reg.schema_at(1, 250).unwrap().columns.len(), 3);
        assert_eq!(reg.schema_at(1, 300).unwrap().columns.len(), 2);
        assert_eq!(reg.schema_at(1, u64::MAX).unwrap().columns.len(), 2);
    }

    #[test]
    fn resent_identical_relation_is_idempotent() {
        let mut reg = Registry::default();
        reg.record_version(&rel(1, &["a"]), 100);
        reg.record_version(&rel(1, &["a"]), 200); // fresh-session re-emit, same shape
        assert_eq!(reg.tables[&1].versions.len(), 1);
    }
}
