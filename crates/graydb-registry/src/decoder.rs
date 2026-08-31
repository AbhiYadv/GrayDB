//! StreamDecoder: incremental frame -> typed-change decoding (the live counterpart of
//! `replay_log`, which stays for offline replay/verification). State persists across
//! feeds: live Relation metadata, the registry, and the currently-open transaction.
//!
//! Rewind contract (paired with graydb_log::tail::LogTail): when the log is truncated
//! back to its durable boundary, only frames of an UNCOMMITTED transaction can vanish
//! (FrameLog::resume invariant). `abort_open_txn` drops that buffer and rewinds the
//! expected sequence; the fresh replication session re-delivers the same transaction.

use crate::pgoutput::{self, Message, RelationDesc};
use crate::{ddl_event_from_row, name_tuple, DdlEvent, Op, Registry, TypedChange};
use anyhow::{Context, Result};
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct DecodedBatch {
    pub changes: Vec<TypedChange>,
    pub ddl_events: Vec<DdlEvent>,
    pub txns: u64,
    /// end_lsn of the last commit in this batch (0 = no commit in batch).
    pub last_commit_lsn: u64,
    /// Monotone global index of the first change in `changes` (synthetic-key base).
    pub first_change_index: u64,
}

struct TxnBuf {
    xid: u32,
    changes: Vec<TypedChange>,
    relations: Vec<RelationDesc>,
    ddl_rows: Vec<DdlEvent>,
}

pub struct StreamDecoder {
    pub registry: Registry,
    live_rel: BTreeMap<u32, RelationDesc>,
    txn: Option<TxnBuf>,
    /// Next frame seq we expect (contiguity check).
    next_seq: u64,
    /// Seq of the last commit frame consumed (rewind target).
    last_commit_seq: u64,
    started: bool,
    /// Changes emitted so far (monotone; feeds deterministic synthetic keys).
    change_counter: u64,
}

impl Default for StreamDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamDecoder {
    pub fn new() -> Self {
        StreamDecoder {
            registry: Registry::default(),
            live_rel: BTreeMap::new(),
            txn: None,
            next_seq: 0,
            last_commit_seq: 0,
            started: false,
            change_counter: 0,
        }
    }

    /// Drop the open (uncommitted) transaction after a log truncation. The next
    /// frames MUST re-start at last_commit_seq + 1 (the fresh session re-delivers).
    pub fn abort_open_txn(&mut self) {
        self.txn = None;
        self.next_seq = if self.started { self.last_commit_seq + 1 } else { 0 };
    }

    pub fn feed(&mut self, frames: &[graydb_log::Frame]) -> Result<DecodedBatch> {
        let mut out = DecodedBatch {
            first_change_index: self.change_counter,
            ..Default::default()
        };
        for frame in frames {
            anyhow::ensure!(
                frame.seq == self.next_seq,
                "frame gap: expected seq {}, got {} (log custody violated)",
                self.next_seq,
                frame.seq
            );
            self.next_seq = frame.seq + 1;
            self.started = true;

            let msg = pgoutput::parse(&frame.payload)
                .with_context(|| format!("decoding frame seq {}", frame.seq))?;
            match msg {
                Message::Begin { xid, .. } => {
                    self.txn = Some(TxnBuf {
                        xid,
                        changes: Vec::new(),
                        relations: Vec::new(),
                        ddl_rows: Vec::new(),
                    });
                }
                Message::Relation(rel) => {
                    if let Some(t) = self.txn.as_mut() {
                        t.relations.push(rel.clone());
                    } else {
                        self.registry.record_version(&rel, frame.lsn_end);
                    }
                    self.live_rel.insert(rel.relid, rel);
                }
                Message::Insert { relid, new } => {
                    let rel = self
                        .live_rel
                        .get(&relid)
                        .context("Insert before Relation")?
                        .clone();
                    let named = name_tuple(&rel, &new);
                    let t = self.txn.as_mut().context("Insert outside transaction")?;
                    if rel.qualified_name() == "graydb.ddl_log" {
                        t.ddl_rows.push(ddl_event_from_row(&named));
                    }
                    t.changes.push(TypedChange {
                        commit_lsn: 0,
                        xid: t.xid,
                        table: rel.qualified_name(),
                        op: Op::Insert,
                        new: Some(named),
                        old: None,
                    });
                }
                Message::Update { relid, old, new } => {
                    let rel = self
                        .live_rel
                        .get(&relid)
                        .context("Update before Relation")?
                        .clone();
                    let t = self.txn.as_mut().context("Update outside transaction")?;
                    t.changes.push(TypedChange {
                        commit_lsn: 0,
                        xid: t.xid,
                        table: rel.qualified_name(),
                        op: Op::Update,
                        new: Some(name_tuple(&rel, &new)),
                        old: old.map(|(_, tv)| name_tuple(&rel, &tv)),
                    });
                }
                Message::Delete { relid, old } => {
                    let rel = self
                        .live_rel
                        .get(&relid)
                        .context("Delete before Relation")?
                        .clone();
                    let t = self.txn.as_mut().context("Delete outside transaction")?;
                    t.changes.push(TypedChange {
                        commit_lsn: 0,
                        xid: t.xid,
                        table: rel.qualified_name(),
                        op: Op::Delete,
                        new: None,
                        old: Some(name_tuple(&rel, &old.1)),
                    });
                }
                Message::Truncate { relids, .. } => {
                    let t = self.txn.as_mut().context("Truncate outside transaction")?;
                    for relid in relids {
                        if let Some(rel) = self.live_rel.get(&relid) {
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
                    let mut t = self.txn.take().context("Commit without Begin")?;
                    self.last_commit_seq = frame.seq;
                    for rel in &t.relations {
                        self.registry.record_version(rel, end_lsn);
                    }
                    for c in &mut t.changes {
                        c.commit_lsn = end_lsn;
                    }
                    for d in &mut t.ddl_rows {
                        d.commit_lsn = end_lsn;
                    }
                    self.change_counter += t.changes.len() as u64;
                    out.changes.append(&mut t.changes);
                    out.ddl_events.append(&mut t.ddl_rows);
                    out.txns += 1;
                    out.last_commit_lsn = end_lsn;
                }
                Message::Origin { .. } | Message::Type { .. } | Message::LogicalMessage { .. } => {}
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod decoder_tests {
    use super::*;
    use bytes::{BufMut, Bytes};

    fn frame(seq: u64, lsn: u64, payload: Vec<u8>, commit: bool) -> graydb_log::Frame {
        graydb_log::Frame {
            seq,
            lsn_start: lsn,
            lsn_end: lsn,
            txn_complete: commit,
            payload: Bytes::from(payload),
        }
    }

    fn begin(xid: u32) -> Vec<u8> {
        let mut b = vec![b'B'];
        b.put_u64(0);
        b.put_i64(0);
        b.put_u32(xid);
        b
    }

    fn commit(end: u64) -> Vec<u8> {
        let mut c = vec![b'C'];
        c.put_u8(0);
        c.put_u64(end - 1);
        c.put_u64(end);
        c.put_i64(0);
        c
    }

    fn relation() -> Vec<u8> {
        let mut r = vec![b'R'];
        r.put_u32(42);
        r.extend(b"app\0t\0");
        r.put_u8(b'd');
        r.put_u16(1);
        r.put_u8(1);
        r.extend(b"id\0");
        r.put_u32(20);
        r.put_i32(-1);
        r
    }

    fn insert(v: &str) -> Vec<u8> {
        let mut i = vec![b'I'];
        i.put_u32(42);
        i.put_u8(b'N');
        i.put_u16(1);
        i.put_u8(b't');
        i.put_u32(v.len() as u32);
        i.extend(v.as_bytes());
        i
    }

    #[test]
    fn incremental_feed_equals_oneshot_and_rewind_reapplies_cleanly() {
        let mut d = StreamDecoder::new();
        // txn 1 committed across two feeds
        let b1 = d
            .feed(&[frame(0, 10, begin(7), false), frame(1, 11, relation(), false)])
            .unwrap();
        assert_eq!(b1.changes.len(), 0);
        let b2 = d
            .feed(&[frame(2, 12, insert("1"), false), frame(3, 20, commit(20), true)])
            .unwrap();
        assert_eq!(b2.changes.len(), 1);
        assert_eq!(b2.changes[0].commit_lsn, 20);
        assert_eq!(b2.txns, 1);

        // open txn 2, then a rewind (truncation) before its commit
        d.feed(&[frame(4, 21, begin(8), false), frame(5, 22, insert("2"), false)])
            .unwrap();
        d.abort_open_txn();
        // fresh session re-delivers the SAME txn with the SAME seqs, then commits
        let b3 = d
            .feed(&[
                frame(4, 21, begin(8), false),
                frame(5, 22, insert("2"), false),
                frame(6, 30, commit(30), true),
            ])
            .unwrap();
        assert_eq!(b3.changes.len(), 1, "aborted txn re-applied exactly once");
        assert_eq!(b3.changes[0].commit_lsn, 30);

        // gap detection
        let err = d.feed(&[frame(99, 31, begin(9), false)]);
        assert!(err.is_err(), "seq gap must fail loudly");
    }
}
