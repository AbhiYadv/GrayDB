//! pgoutput protocol v1 message parser. Frames hold these bytes raw (I2: the log is
//! the truth); this module is the ONLY place that interprets them. Text-format tuple
//! values are kept as the source rendered them — the Type Interpretation Contract v0.

use anyhow::{bail, Result};
use bytes::Buf;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Begin {
        final_lsn: u64,
        commit_ts: i64,
        xid: u32,
    },
    Commit {
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        commit_ts: i64,
    },
    Origin {
        commit_lsn: u64,
        name: String,
    },
    Relation(RelationDesc),
    /// Type metadata for non-builtin types (arrives before first use).
    Type {
        type_oid: u32,
        namespace: String,
        name: String,
    },
    Insert {
        relid: u32,
        new: Vec<TupleValue>,
    },
    Update {
        relid: u32,
        /// 'K' = old key, 'O' = old full row (REPLICA IDENTITY FULL), None = absent.
        old: Option<(u8, Vec<TupleValue>)>,
        new: Vec<TupleValue>,
    },
    Delete {
        relid: u32,
        /// 'K' = key columns, 'O' = full old row.
        old: (u8, Vec<TupleValue>),
    },
    Truncate {
        options: u8,
        relids: Vec<u32>,
    },
    /// Logical decoding message (pg_logical_emit_message).
    LogicalMessage {
        transactional: bool,
        lsn: u64,
        prefix: String,
        payload: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationDesc {
    pub relid: u32,
    /// Empty namespace on the wire means pg_catalog.
    pub namespace: String,
    pub name: String,
    pub replident: u8,
    pub columns: Vec<ColumnDesc>,
}

impl RelationDesc {
    pub fn qualified_name(&self) -> String {
        let ns = if self.namespace.is_empty() {
            "pg_catalog"
        } else {
            &self.namespace
        };
        format!("{ns}.{}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDesc {
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
    /// Part of the replica identity / key.
    pub is_key: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TupleValue {
    Null,
    /// Unchanged TOASTed column omitted from the image (Amendment A A5.3:
    /// reconstructed from the prior materialized version — SP4's job).
    UnchangedToast,
    /// Text-format value exactly as the source rendered it.
    Text(String),
    /// Binary-format value (we do not request binary in v1; kept for completeness).
    Binary(Vec<u8>),
}

pub fn parse(payload: &[u8]) -> Result<Message> {
    let mut b = payload;
    anyhow::ensure!(!b.is_empty(), "empty pgoutput payload");
    let tag = b.get_u8();
    let msg = match tag {
        b'B' => Message::Begin {
            final_lsn: b.get_u64(),
            commit_ts: b.get_i64(),
            xid: b.get_u32(),
        },
        b'C' => Message::Commit {
            flags: b.get_u8(),
            commit_lsn: b.get_u64(),
            end_lsn: b.get_u64(),
            commit_ts: b.get_i64(),
        },
        b'O' => Message::Origin {
            commit_lsn: b.get_u64(),
            name: get_cstr(&mut b)?,
        },
        b'R' => {
            let relid = b.get_u32();
            let namespace = get_cstr(&mut b)?;
            let name = get_cstr(&mut b)?;
            let replident = b.get_u8();
            let ncols = b.get_u16();
            let mut columns = Vec::with_capacity(ncols as usize);
            for _ in 0..ncols {
                let flags = b.get_u8();
                columns.push(ColumnDesc {
                    is_key: flags & 1 != 0,
                    name: get_cstr(&mut b)?,
                    type_oid: b.get_u32(),
                    typmod: b.get_i32(),
                });
            }
            Message::Relation(RelationDesc {
                relid,
                namespace,
                name,
                replident,
                columns,
            })
        }
        b'Y' => Message::Type {
            type_oid: b.get_u32(),
            namespace: get_cstr(&mut b)?,
            name: get_cstr(&mut b)?,
        },
        b'I' => {
            let relid = b.get_u32();
            let kind = b.get_u8();
            anyhow::ensure!(kind == b'N', "Insert without new tuple (kind {kind})");
            Message::Insert {
                relid,
                new: parse_tuple(&mut b)?,
            }
        }
        b'U' => {
            let relid = b.get_u32();
            let mut kind = b.get_u8();
            let mut old = None;
            if kind == b'K' || kind == b'O' {
                old = Some((kind, parse_tuple(&mut b)?));
                kind = b.get_u8();
            }
            anyhow::ensure!(kind == b'N', "Update without new tuple (kind {kind})");
            Message::Update {
                relid,
                old,
                new: parse_tuple(&mut b)?,
            }
        }
        b'D' => {
            let relid = b.get_u32();
            let kind = b.get_u8();
            anyhow::ensure!(
                kind == b'K' || kind == b'O',
                "Delete with unexpected tuple kind {kind}"
            );
            Message::Delete {
                relid,
                old: (kind, parse_tuple(&mut b)?),
            }
        }
        b'T' => {
            let nrels = b.get_u32();
            let options = b.get_u8();
            let mut relids = Vec::with_capacity(nrels as usize);
            for _ in 0..nrels {
                relids.push(b.get_u32());
            }
            Message::Truncate { options, relids }
        }
        b'M' => {
            let flags = b.get_u8();
            let lsn = b.get_u64();
            let prefix = get_cstr(&mut b)?;
            let len = b.get_u32() as usize;
            anyhow::ensure!(b.remaining() >= len, "truncated logical message");
            let payload = b[..len].to_vec();
            Message::LogicalMessage {
                transactional: flags & 1 != 0,
                lsn,
                prefix,
                payload,
            }
        }
        other => bail!("unknown pgoutput message tag {:?}", other as char),
    };
    Ok(msg)
}

fn parse_tuple(b: &mut &[u8]) -> Result<Vec<TupleValue>> {
    let ncols = b.get_u16();
    let mut out = Vec::with_capacity(ncols as usize);
    for _ in 0..ncols {
        let kind = b.get_u8();
        out.push(match kind {
            b'n' => TupleValue::Null,
            b'u' => TupleValue::UnchangedToast,
            b't' => {
                let len = b.get_u32() as usize;
                anyhow::ensure!(b.remaining() >= len, "truncated text tuple value");
                let v = String::from_utf8_lossy(&b[..len]).into_owned();
                b.advance(len);
                TupleValue::Text(v)
            }
            b'b' => {
                let len = b.get_u32() as usize;
                anyhow::ensure!(b.remaining() >= len, "truncated binary tuple value");
                let v = b[..len].to_vec();
                b.advance(len);
                TupleValue::Binary(v)
            }
            other => bail!("unknown tuple value kind {:?}", other as char),
        });
    }
    Ok(out)
}

fn get_cstr(b: &mut &[u8]) -> Result<String> {
    let nul = b
        .iter()
        .position(|&x| x == 0)
        .ok_or_else(|| anyhow::anyhow!("unterminated cstring in pgoutput message"))?;
    let s = String::from_utf8_lossy(&b[..nul]).into_owned();
    b.advance(nul + 1);
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn parses_relation_and_insert() {
        // Hand-built Relation: relid 42, ns "app", name "t", replident 'd', 2 cols.
        let mut r = vec![b'R'];
        r.put_u32(42);
        r.extend(b"app\0");
        r.extend(b"t\0");
        r.put_u8(b'd');
        r.put_u16(2);
        r.put_u8(1);
        r.extend(b"id\0");
        r.put_u32(20);
        r.put_i32(-1);
        r.put_u8(0);
        r.extend(b"v\0");
        r.put_u32(25);
        r.put_i32(-1);
        let Message::Relation(rel) = parse(&r).unwrap() else {
            panic!("not a relation")
        };
        assert_eq!(rel.relid, 42);
        assert_eq!(rel.qualified_name(), "app.t");
        assert_eq!(rel.columns.len(), 2);
        assert!(rel.columns[0].is_key);
        assert_eq!(rel.columns[1].name, "v");

        // Insert: relid 42, N, 2 cols: text "7", null.
        let mut i = vec![b'I'];
        i.put_u32(42);
        i.put_u8(b'N');
        i.put_u16(2);
        i.put_u8(b't');
        i.put_u32(1);
        i.extend(b"7");
        i.put_u8(b'n');
        let Message::Insert { relid, new } = parse(&i).unwrap() else {
            panic!("not an insert")
        };
        assert_eq!(relid, 42);
        assert_eq!(new[0], TupleValue::Text("7".into()));
        assert_eq!(new[1], TupleValue::Null);
    }

    #[test]
    fn parses_commit_boundary() {
        let mut c = vec![b'C'];
        c.put_u8(0);
        c.put_u64(100);
        c.put_u64(200);
        c.put_i64(0);
        let Message::Commit { commit_lsn, end_lsn, .. } = parse(&c).unwrap() else {
            panic!("not a commit")
        };
        assert_eq!((commit_lsn, end_lsn), (100, 200));
    }
}
