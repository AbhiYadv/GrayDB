//! graydb-search: tantivy indexes per declared table+columns, applied in commit-LSN
//! batches. Never commits mid-transaction; index freshness = last committed batch LSN
//! (surfaced as applied_lsn in graydb.stat_replication, D-014 naming).
//!
//! Document identity: keyed tables use the replica-identity values; keyless
//! (append-only) tables use a caller-supplied deterministic synthetic key (the
//! replay's global change index — stable across replays, so re-apply after a crash
//! is delete+re-add idempotent instead of duplicating).

use anyhow::{Context, Result};
use graydb_registry::pgoutput::TupleValue;
use graydb_registry::{Op, TypedChange};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{
    Field, Schema, TantivyDocument, Value, STORED, STRING, TEXT,
};
use tantivy::{Index, IndexSettings, IndexWriter, Term};

pub mod retry_dir;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchMeta {
    pub table: String,
    pub columns: Vec<String>,
    /// Last committed batch LSN — this index answers as of here.
    pub applied_lsn: u64,
    pub commits: u64,
}

pub struct SearchStore {
    dir: PathBuf,
    pub table: String,
    pub columns: Vec<String>,
    /// Replica-identity column names; empty = keyless (append-only).
    key_columns: Vec<String>,
    index: Index,
    writer: IndexWriter,
    key_field: Field,
    lsn_field: Field,
    text_fields: Vec<Field>,
    pub meta: SearchMeta,
}

impl SearchStore {
    pub fn create(
        dir: &Path,
        table: &str,
        columns: &[String],
        key_columns: &[String],
    ) -> Result<Self> {
        if dir.exists() {
            std::fs::remove_dir_all(dir).ok();
        }
        std::fs::create_dir_all(dir)?;
        let mut sb = Schema::builder();
        let key_field = sb.add_text_field("__key", STRING | STORED);
        let lsn_field = sb.add_u64_field("__lsn", STORED);
        let text_fields: Vec<Field> = columns
            .iter()
            .map(|c| sb.add_text_field(c, TEXT | STORED))
            .collect();
        let schema = sb.build();
        let index = Index::create(
            retry_dir::RetryDirectory::open(dir)?,
            schema,
            IndexSettings::default(),
        )
        .context("creating tantivy index")?;
        let writer = index.writer(50_000_000).context("tantivy writer")?;
        Ok(SearchStore {
            dir: dir.to_path_buf(),
            table: table.to_string(),
            columns: columns.to_vec(),
            key_columns: key_columns.to_vec(),
            index,
            writer,
            key_field,
            lsn_field,
            text_fields,
            meta: SearchMeta {
                table: table.to_string(),
                columns: columns.to_vec(),
                applied_lsn: 0,
                commits: 0,
            },
        })
    }

    pub fn is_keyed(&self) -> bool {
        !self.key_columns.is_empty()
    }

    fn key_from_image(&self, named: &[(String, TupleValue)]) -> Result<String> {
        let mut parts = Vec::with_capacity(self.key_columns.len());
        for kc in &self.key_columns {
            let v = named
                .iter()
                .find(|(n, _)| n == kc)
                .and_then(|(_, v)| match v {
                    TupleValue::Text(s) => Some(s.as_str()),
                    _ => None,
                })
                .with_context(|| format!("{}: key column {kc} missing from image", self.table))?;
            parts.push(v.to_string());
        }
        Ok(parts.join("\u{1f}"))
    }

    /// `values` is aligned with the declared columns (self.text_fields order).
    fn add_doc<S: AsRef<str>>(&mut self, key: &str, values: &[Option<S>], lsn: u64) -> Result<()> {
        let mut d = TantivyDocument::default();
        d.add_text(self.key_field, key);
        d.add_u64(self.lsn_field, lsn);
        for (field, value) in self.text_fields.iter().zip(values.iter()) {
            if let Some(v) = value {
                d.add_text(*field, v.as_ref());
            }
        }
        self.writer.add_document(d)?;
        Ok(())
    }

    fn delete_key(&mut self, key: &str) {
        self.writer
            .delete_term(Term::from_field_text(self.key_field, key));
    }

    /// Project the declared columns out of a change image (owned, aligned with
    /// text_fields order, so a &mut borrow can follow immediately).
    fn project(&self, named: &[(String, TupleValue)]) -> Vec<Option<String>> {
        self.columns
            .iter()
            .map(|c| {
                named.iter().find(|(n, _)| n == c).and_then(|(_, v)| match v {
                    TupleValue::Text(s) => Some(s.clone()),
                    _ => None,
                })
            })
            .collect()
    }

    /// Bulk-index one backfill row (values pre-projected by the caller, aligned with
    /// the declared columns).
    pub fn index_backfill_row(
        &mut self,
        key: &str,
        values: &[Option<&str>],
        lsn0: u64,
    ) -> Result<()> {
        // delete+add: idempotent under re-runs.
        self.delete_key(key);
        self.add_doc(key, values, lsn0)
    }

    /// Apply one typed change. `synthetic_key` is used for keyless tables and MUST be
    /// deterministic across replays.
    pub fn apply(&mut self, change: &TypedChange, synthetic_key: &str) -> Result<()> {
        match change.op {
            Op::Insert => {
                let named = change.new.as_ref().context("insert without image")?;
                let key = if self.is_keyed() {
                    self.key_from_image(named)?
                } else {
                    synthetic_key.to_string()
                };
                self.delete_key(&key); // replay idempotency
                let projected = self.project(named);
                self.add_doc(&key, &projected, change.commit_lsn)?;
            }
            Op::Update => {
                anyhow::ensure!(self.is_keyed(), "{}: update on keyless index", self.table);
                let named = change.new.as_ref().context("update without new image")?;
                let old_key = match &change.old {
                    Some(old) => self.key_from_image(old)?,
                    None => self.key_from_image(named)?,
                };
                self.delete_key(&old_key);
                let new_key = self.key_from_image(named)?;
                let projected = self.project(named);
                self.add_doc(&new_key, &projected, change.commit_lsn)?;
            }
            Op::Delete => {
                anyhow::ensure!(self.is_keyed(), "{}: delete on keyless index", self.table);
                let old = change.old.as_ref().context("delete without old image")?;
                let key = self.key_from_image(old)?;
                self.delete_key(&key);
            }
            Op::Truncate => {
                self.writer.delete_all_documents()?;
            }
        }
        Ok(())
    }

    /// Commit the current LSN batch. NEVER call mid-transaction — callers commit only
    /// at commit-LSN boundaries (the apply driver guarantees it).
    pub fn commit_batch(&mut self, applied_lsn: u64) -> Result<()> {
        self.writer.commit().context("tantivy commit")?;
        self.meta.applied_lsn = applied_lsn;
        self.meta.commits += 1;
        std::fs::write(
            self.dir.join("graydb-meta.json"),
            serde_json::to_vec_pretty(&self.meta)?,
        )?;
        Ok(())
    }

    pub fn num_docs(&self) -> Result<u64> {
        let reader = self.index.reader()?;
        reader.reload()?;
        Ok(reader.searcher().num_docs())
    }

    /// BM25 search over the declared columns; returns (key, lsn, score), best first.
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<(String, u64, f32)>> {
        let reader = self.index.reader()?;
        reader.reload()?;
        let searcher = reader.searcher();
        let qp = QueryParser::for_index(&self.index, self.text_fields.clone());
        let q = qp.parse_query(query).context("parsing search query")?;
        let top = searcher.search(&q, &TopDocs::with_limit(limit).order_by_score())?;
        let mut out = Vec::with_capacity(top.len());
        for (score, addr) in top {
            let doc: TantivyDocument = searcher.doc(addr)?;
            let key = doc
                .get_first(self.key_field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let lsn = doc
                .get_first(self.lsn_field)
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            out.push((key, lsn, score));
        }
        Ok(out)
    }
}

/// Read-only opener for a finished index directory (the SP6 reader's search path):
/// no writer, no lock contention with an active pump, applied_lsn from the
/// persisted watermark.
pub struct SearchReader {
    index: Index,
    key_field: Field,
    text_fields: Vec<Field>,
    pub meta: SearchMeta,
}

impl SearchReader {
    pub fn open(dir: &Path) -> Result<Self> {
        let meta: SearchMeta = serde_json::from_slice(
            &std::fs::read(dir.join("graydb-meta.json"))
                .with_context(|| format!("reading search meta in {}", dir.display()))?,
        )?;
        let index = Index::open(retry_dir::RetryDirectory::open(dir)?)
            .context("opening tantivy index read-only")?;
        let schema = index.schema();
        let key_field = schema.get_field("__key").context("__key field")?;
        let text_fields: Vec<Field> = meta
            .columns
            .iter()
            .map(|c| schema.get_field(c).with_context(|| format!("field {c}")))
            .collect::<Result<_>>()?;
        Ok(SearchReader {
            index,
            key_field,
            text_fields,
            meta,
        })
    }

    /// BM25 over the declared columns; (key, score), best first.
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<(String, f32)>> {
        let reader = self.index.reader()?;
        reader.reload()?;
        let searcher = reader.searcher();
        let qp = QueryParser::for_index(&self.index, self.text_fields.clone());
        let q = qp.parse_query(query).context("parsing search query")?;
        let top = searcher.search(&q, &TopDocs::with_limit(limit).order_by_score())?;
        let mut out = Vec::with_capacity(top.len());
        for (score, addr) in top {
            let doc: TantivyDocument = searcher.doc(addr)?;
            let key = doc
                .get_first(self.key_field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            out.push((key, score));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graydb_registry::pgoutput::TupleValue;

    fn img(id: i64, name: &str) -> Vec<(String, TupleValue)> {
        vec![
            ("id".to_string(), TupleValue::Text(id.to_string())),
            ("name".to_string(), TupleValue::Text(name.to_string())),
        ]
    }

    fn store(dir_tag: &str) -> SearchStore {
        // Workspace-relative, not %TEMP%: corporate AV on Windows intercepts fresh
        // file creates under the temp dir and tantivy's .del writes get EACCES.
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-tmp")
            .join(format!("gdb-search-{dir_tag}-{}", std::process::id()));
        SearchStore::create(&dir, "app.t", &["name".to_string()], &["id".to_string()]).unwrap()
    }

    #[test]
    fn update_is_delete_plus_readd() {
        let mut s = store("upd");
        s.apply(
            &TypedChange {
                commit_lsn: 10, xid: 1, table: "app.t".into(), op: Op::Insert,
                new: Some(img(1, "zephyr quokka")), old: None,
            },
            "",
        ).unwrap();
        s.apply(
            &TypedChange {
                commit_lsn: 20, xid: 2, table: "app.t".into(), op: Op::Update,
                new: Some(img(1, "xylophone marmot")), old: None,
            },
            "",
        ).unwrap();
        s.commit_batch(20).unwrap();
        assert_eq!(s.num_docs().unwrap(), 1);
        assert_eq!(s.search("xylophone", 10).unwrap().len(), 1);
        assert_eq!(s.search("zephyr", 10).unwrap().len(), 0, "old version must be gone");
        assert_eq!(s.meta.applied_lsn, 20);
    }

    #[test]
    fn replay_reapply_is_idempotent() {
        let mut s = store("idem");
        let ins = TypedChange {
            commit_lsn: 10, xid: 1, table: "app.t".into(), op: Op::Insert,
            new: Some(img(7, "kumquat")), old: None,
        };
        s.apply(&ins, "").unwrap();
        s.apply(&ins, "").unwrap(); // crash-window re-apply
        s.commit_batch(10).unwrap();
        assert_eq!(s.num_docs().unwrap(), 1, "re-apply must not duplicate");
    }
}
