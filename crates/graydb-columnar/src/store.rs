//! TableStore: parquet segments (zstd, dictionary) + roaring delete-bitmap sidecars
//! + per-segment LSN ranges. Update = bitmap-mark + reinsert (never in-place).
//! Visibility at L: insert_lsn <= L && !(deleted_by <= L) — exactly the predicate
//! the wedge's T5 trial measured. Values live as the source's text renderings;
//! int/float/bool columns are stored typed for scan performance (D-013).

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{
    ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, StringArray, StringBuilder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use graydb_registry::pgoutput::TupleValue;
use graydb_registry::{Op, TypedChange};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const OPEN_SEGMENT: u32 = u32::MAX;
const LSN_COL: &str = "__gdb_lsn";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    pub type_oid: u32,
    pub is_key: bool,
}

/// Arrow schema for a column set, always ending with the `__gdb_lsn` UInt64 column.
pub fn arrow_schema_for(columns: &[ColumnSpec]) -> SchemaRef {
    let mut fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, arrow_type(c.type_oid), true))
        .collect();
    fields.push(Field::new(LSN_COL, DataType::UInt64, false));
    Arc::new(Schema::new(fields))
}

/// PG type oid -> storage type (per-type mapping table v0, Amendment A A5.2 / D-013):
/// ints -> Int64, floats -> Float64, bool -> Boolean, EVERYTHING else -> Utf8 in the
/// source's own text rendering (numeric stays text: exactness over speed in v1).
fn arrow_type(oid: u32) -> DataType {
    match oid {
        16 => DataType::Boolean,
        20 | 21 | 23 | 26 => DataType::Int64, // int8/int2/int4/oid
        700 | 701 => DataType::Float64,
        _ => DataType::Utf8,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub id: u32,
    pub rows: u64,
    pub lsn_min: u64,
    pub lsn_max: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Sidecar {
    /// (row index, delete commit LSN); the roaring bitmap is the in-memory mask.
    deletes: Vec<(u32, u64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreManifest {
    pub table: String,
    pub columns: Vec<ColumnSpec>,
    pub applied_lsn: u64,
    pub segments: Vec<SegmentMeta>,
}

struct OpenRow {
    values: Vec<Option<String>>,
    insert_lsn: u64,
    delete_lsn: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Loc {
    seg: u32,
    row: u32,
}

pub struct TableStore {
    dir: PathBuf,
    pub table: String,
    columns: Vec<ColumnSpec>,
    key_idx: Vec<usize>,
    arrow_schema: SchemaRef,
    flush_rows: usize,
    open: Vec<OpenRow>,
    segments: Vec<SegmentMeta>,
    sidecars: BTreeMap<u32, (Sidecar, RoaringBitmap)>,
    index: HashMap<Vec<u8>, Loc>,
    pub applied_lsn: u64,
    /// One-segment row cache for TOAST reconstruction reads.
    seg_cache: Option<(u32, Vec<Vec<Option<String>>>)>,
    /// Live (visible-at-head) row count, maintained O(1) (R1 prerequisite P4).
    live_rows: u64,
}

/// A point-in-time view of one flushed segment, for the streaming reader (R1/P3).
#[derive(Debug, Clone)]
pub struct SegmentSnapshot {
    pub meta: SegmentMeta,
    pub path: PathBuf,
    /// (row index, delete commit LSN) pairs accumulated so far.
    pub deletes: Vec<(u32, u64)>,
}

impl TableStore {
    pub fn create(dir: &Path, table: &str, columns: Vec<ColumnSpec>, flush_rows: usize) -> Result<Self> {
        anyhow::ensure!(!columns.is_empty(), "no columns for {table}");
        if dir.exists() {
            std::fs::remove_dir_all(dir).ok();
        }
        std::fs::create_dir_all(dir)?;
        let key_idx: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.is_key)
            .map(|(i, _)| i)
            .collect();
        let schema = arrow_schema_for(&columns);
        Ok(TableStore {
            dir: dir.to_path_buf(),
            table: table.to_string(),
            columns,
            key_idx,
            arrow_schema: schema,
            flush_rows,
            open: Vec::new(),
            segments: Vec::new(),
            sidecars: BTreeMap::new(),
            index: HashMap::new(),
            applied_lsn: 0,
            seg_cache: None,
            live_rows: 0,
        })
    }

    pub fn has_key(&self) -> bool {
        !self.key_idx.is_empty()
    }

    /// Visible-at-head row count, O(1).
    pub fn visible_rows(&self) -> u64 {
        self.live_rows
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    /// Point-in-time view of all FLUSHED segments (paths are immutable parquet files;
    /// deletes are copied so the reader holds no lock while scanning).
    pub fn segments_snapshot(&self) -> Vec<SegmentSnapshot> {
        self.segments
            .iter()
            .map(|meta| SegmentSnapshot {
                meta: meta.clone(),
                path: self.segment_path(meta.id),
                deletes: self
                    .sidecars
                    .get(&meta.id)
                    .map(|(s, _)| s.deletes.clone())
                    .unwrap_or_default(),
            })
            .collect()
    }

    /// The not-yet-flushed open rows visible at `lsn`, as one typed batch (the
    /// memtable overlay the reader unions with flushed segments — R1/P2).
    pub fn open_rows_batch(&self, lsn: u64) -> Result<Option<RecordBatch>> {
        let visible: Vec<&OpenRow> = self
            .open
            .iter()
            .filter(|r| r.insert_lsn <= lsn && !r.delete_lsn.is_some_and(|d| d <= lsn))
            .collect();
        if visible.is_empty() {
            return Ok(None);
        }
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len() + 1);
        for (i, col) in self.columns.iter().enumerate() {
            arrays.push(build_array(&col.name, arrow_type(col.type_oid), &visible, i)?);
        }
        let mut lsns = UInt64Builder::with_capacity(visible.len());
        for r in &visible {
            lsns.append_value(r.insert_lsn);
        }
        arrays.push(Arc::new(lsns.finish()));
        Ok(Some(RecordBatch::try_new(self.arrow_schema.clone(), arrays)?))
    }

    fn key_of(&self, values: &[Option<String>]) -> Vec<u8> {
        let mut k = Vec::new();
        for &i in &self.key_idx {
            match &values[i] {
                Some(s) => {
                    k.push(1);
                    k.extend_from_slice(s.as_bytes());
                }
                None => k.push(0),
            }
            k.push(0x1F);
        }
        k
    }

    /// Bulk-load one staged COPY part at LSN0 (the backfill base).
    pub fn load_copy_part(&mut self, data: &[u8], lsn0: u64) -> Result<u64> {
        let mut n = 0u64;
        for line in graydb_check_split(data) {
            let values = crate::copytext::parse_copy_line(line);
            anyhow::ensure!(
                values.len() == self.columns.len(),
                "{}: COPY row has {} cols, store has {}",
                self.table,
                values.len(),
                self.columns.len()
            );
            self.insert_row(values, lsn0);
            n += 1;
        }
        self.applied_lsn = self.applied_lsn.max(lsn0);
        Ok(n)
    }

    fn insert_row(&mut self, values: Vec<Option<String>>, lsn: u64) {
        if self.has_key() {
            let key = self.key_of(&values);
            self.index.insert(
                key,
                Loc {
                    seg: OPEN_SEGMENT,
                    row: self.open.len() as u32,
                },
            );
        }
        self.open.push(OpenRow {
            values,
            insert_lsn: lsn,
            delete_lsn: None,
        });
        self.live_rows += 1;
    }

    fn mark_deleted(&mut self, loc: Loc, lsn: u64) {
        self.live_rows = self.live_rows.saturating_sub(1);
        if loc.seg == OPEN_SEGMENT {
            self.open[loc.row as usize].delete_lsn = Some(lsn);
        } else {
            let (sidecar, mask) = self.sidecars.entry(loc.seg).or_default();
            sidecar.deletes.push((loc.row, lsn));
            mask.insert(loc.row);
        }
    }

    fn tuple_to_values(
        &mut self,
        named: &[(String, TupleValue)],
        prior: Option<Loc>,
    ) -> Result<Vec<Option<String>>> {
        anyhow::ensure!(
            named.len() == self.columns.len(),
            "{}: change has {} cols, store has {} (schema drift not yet materialized — SP4 scope is fixed-shape apply)",
            self.table,
            named.len(),
            self.columns.len()
        );
        let mut out = Vec::with_capacity(named.len());
        for (i, (name, value)) in named.iter().enumerate() {
            anyhow::ensure!(
                *name == self.columns[i].name,
                "{}: column {i} is {} in stream but {} in store",
                self.table,
                name,
                self.columns[i].name
            );
            out.push(match value {
                TupleValue::Text(s) => Some(s.clone()),
                TupleValue::Null => None,
                TupleValue::UnchangedToast => {
                    // Amendment A A5.3: reconstruct from the prior materialized version.
                    let loc = prior.ok_or_else(|| {
                        anyhow!("{}: unchanged-TOAST column {name} with no prior version", self.table)
                    })?;
                    self.fetch_value(loc, i)?
                }
                TupleValue::Binary(_) => bail!("binary tuple format not requested in v1"),
            });
        }
        Ok(out)
    }

    fn fetch_value(&mut self, loc: Loc, col: usize) -> Result<Option<String>> {
        if loc.seg == OPEN_SEGMENT {
            return Ok(self.open[loc.row as usize].values[col].clone());
        }
        if self.seg_cache.as_ref().map(|(id, _)| *id) != Some(loc.seg) {
            let rows = self.read_segment_rows(loc.seg)?;
            self.seg_cache = Some((loc.seg, rows));
        }
        Ok(self.seg_cache.as_ref().expect("just cached").1[loc.row as usize][col].clone())
    }

    /// Apply one typed change in commit order.
    pub fn apply(&mut self, change: &TypedChange) -> Result<()> {
        let lsn = change.commit_lsn;
        match change.op {
            Op::Insert => {
                let named = change.new.as_ref().context("insert without new image")?;
                let values = self.tuple_to_values(named, None)?;
                self.insert_row(values, lsn);
            }
            Op::Update => {
                anyhow::ensure!(self.has_key(), "{}: update on keyless store", self.table);
                let named = change.new.as_ref().context("update without new image")?;
                // Old image present only when the identity changed (or RI FULL);
                // otherwise the new image carries the same key.
                let old_key = match &change.old {
                    Some(old) => self.key_of(&image_values(old)),
                    None => self.key_of(&image_values(named)),
                };
                let prior = self.index.get(&old_key).copied();
                let values = self.tuple_to_values(named, prior)?;
                if let Some(loc) = prior {
                    self.mark_deleted(loc, lsn);
                    self.index.remove(&old_key);
                } else {
                    bail!("{}: update for unknown key (invariant breach)", self.table);
                }
                self.insert_row(values, lsn);
            }
            Op::Delete => {
                anyhow::ensure!(self.has_key(), "{}: delete on keyless store", self.table);
                let old = change.old.as_ref().context("delete without old image/key")?;
                let key = self.key_of(&image_values(old));
                let loc = self
                    .index
                    .remove(&key)
                    .ok_or_else(|| anyhow!("{}: delete for unknown key", self.table))?;
                self.mark_deleted(loc, lsn);
            }
            Op::Truncate => {
                let locs: Vec<Loc> = self.index.values().copied().collect();
                for loc in locs {
                    self.mark_deleted(loc, lsn);
                }
                self.index.clear();
            }
        }
        self.applied_lsn = self.applied_lsn.max(lsn);
        if self.open.len() >= self.flush_rows {
            self.flush()?;
        }
        Ok(())
    }

    /// Write the open rows as the next parquet segment (zstd, dictionary on,
    /// LSN range in footer metadata) + its delete sidecar.
    pub fn flush(&mut self) -> Result<()> {
        if self.open.is_empty() {
            return Ok(());
        }
        let seg_id = self.segments.len() as u32;
        let rows = std::mem::take(&mut self.open);
        let lsn_min = rows.iter().map(|r| r.insert_lsn).min().unwrap_or(0);
        let lsn_max = rows.iter().map(|r| r.insert_lsn).max().unwrap_or(0);

        // Column arrays.
        let row_refs: Vec<&OpenRow> = rows.iter().collect();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len() + 1);
        for (i, col) in self.columns.iter().enumerate() {
            arrays.push(build_array(&col.name, arrow_type(col.type_oid), &row_refs, i)?);
        }
        let mut lsns = UInt64Builder::with_capacity(rows.len());
        for r in &rows {
            lsns.append_value(r.insert_lsn);
        }
        arrays.push(Arc::new(lsns.finish()));
        let batch = RecordBatch::try_new(self.arrow_schema.clone(), arrays)?;

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_dictionary_enabled(true)
            .set_key_value_metadata(Some(vec![
                kv("graydb.table", &self.table),
                kv("graydb.lsn_min", &lsn_min.to_string()),
                kv("graydb.lsn_max", &lsn_max.to_string()),
            ]))
            .build();
        let file = std::fs::File::create(self.segment_path(seg_id))?;
        let mut writer = ArrowWriter::try_new(file, self.arrow_schema.clone(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        // Deletes observed while the segment was still open move into its sidecar;
        // survivors get their index entries repointed to the flushed location.
        let (sidecar, mask) = self.sidecars.entry(seg_id).or_default();
        for (row_idx, row) in rows.iter().enumerate() {
            if let Some(dlsn) = row.delete_lsn {
                sidecar.deletes.push((row_idx as u32, dlsn));
                mask.insert(row_idx as u32);
            }
        }
        if self.has_key() {
            for (row_idx, row) in rows.iter().enumerate() {
                if row.delete_lsn.is_none() {
                    let key = self.key_of(&row.values);
                    self.index.insert(
                        key,
                        Loc {
                            seg: seg_id,
                            row: row_idx as u32,
                        },
                    );
                }
            }
        }
        self.segments.push(SegmentMeta {
            id: seg_id,
            rows: rows.len() as u64,
            lsn_min,
            lsn_max,
        });
        tracing::info!(table = %self.table, seg_id, rows = rows.len(), "segment flushed");
        Ok(())
    }

    /// Flush + persist sidecars and manifest. Store is fully on disk afterwards.
    pub fn finalize(&mut self) -> Result<StoreManifest> {
        self.flush()?;
        for (seg_id, (sidecar, _)) in &self.sidecars {
            std::fs::write(
                self.sidecar_path(*seg_id),
                serde_json::to_vec_pretty(sidecar)?,
            )?;
        }
        let manifest = StoreManifest {
            table: self.table.clone(),
            columns: self.columns.clone(),
            applied_lsn: self.applied_lsn,
            segments: self.segments.clone(),
        };
        std::fs::write(
            self.dir.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )?;
        Ok(manifest)
    }

    fn segment_path(&self, id: u32) -> PathBuf {
        self.dir.join(format!("seg-{id:06}.parquet"))
    }

    fn sidecar_path(&self, id: u32) -> PathBuf {
        self.dir.join(format!("seg-{id:06}.del.json"))
    }

    fn read_segment_rows(&self, seg_id: u32) -> Result<Vec<Vec<Option<String>>>> {
        let file = std::fs::File::open(self.segment_path(seg_id))
            .with_context(|| format!("opening segment {seg_id} of {}", self.table))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        for batch in reader {
            let batch = batch?;
            append_rendered_rows(&batch, self.columns.len(), &mut rows, None, u64::MAX)?;
        }
        Ok(rows)
    }

    /// All rows visible at `lsn`, text-rendered, in no particular order.
    pub fn scan_at(&self, lsn: u64) -> Result<Vec<Vec<Option<String>>>> {
        let mut out: Vec<Vec<Option<String>>> = Vec::new();
        for seg in &self.segments {
            if seg.lsn_min > lsn {
                continue;
            }
            // Rows deleted at-or-before lsn are invisible.
            let mut deleted_by: RoaringBitmap = RoaringBitmap::new();
            if let Some((sidecar, _)) = self.sidecars.get(&seg.id) {
                for (row, dlsn) in &sidecar.deletes {
                    if *dlsn <= lsn {
                        deleted_by.insert(*row);
                    }
                }
            }
            let file = std::fs::File::open(self.segment_path(seg.id))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
            let mut base = 0u32;
            for batch in reader {
                let batch = batch?;
                append_rendered_rows(
                    &batch,
                    self.columns.len(),
                    &mut out,
                    Some((&deleted_by, base)),
                    lsn,
                )?;
                base += batch.num_rows() as u32;
            }
        }
        for row in &self.open {
            if row.insert_lsn <= lsn && !row.delete_lsn.is_some_and(|d| d <= lsn) {
                out.push(row.values.clone());
            }
        }
        Ok(out)
    }
}

fn image_values(named: &[(String, TupleValue)]) -> Vec<Option<String>> {
    named
        .iter()
        .map(|(_, v)| match v {
            TupleValue::Text(s) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

fn kv(key: &str, value: &str) -> parquet::file::metadata::KeyValue {
    parquet::file::metadata::KeyValue::new(key.to_string(), value.to_string())
}

fn build_array(
    name: &str,
    dt: DataType,
    rows: &[&OpenRow],
    col: usize,
) -> Result<ArrayRef> {
    let parse_err = |v: &str| anyhow!("column {name}: cannot parse {v:?}");
    Ok(match dt {
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for r in rows {
                match &r.values[col] {
                    Some(v) => b.append_value(v.parse::<i64>().map_err(|_| parse_err(v))?),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for r in rows {
                match &r.values[col] {
                    Some(v) => b.append_value(v.parse::<f64>().map_err(|_| parse_err(v))?),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for r in rows {
                match r.values[col].as_deref() {
                    Some("t") | Some("true") => b.append_value(true),
                    Some("f") | Some("false") => b.append_value(false),
                    Some(v) => bail!("column {name}: cannot parse bool {v:?}"),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        _ => {
            let mut b = StringBuilder::new();
            for r in rows {
                match &r.values[col] {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    })
}

/// Render batch rows back to source-text form, applying visibility if given:
/// skip rows in `deleted` (offset by `base`) and rows with insert_lsn > max_lsn.
fn append_rendered_rows(
    batch: &RecordBatch,
    ncols: usize,
    out: &mut Vec<Vec<Option<String>>>,
    visibility: Option<(&RoaringBitmap, u32)>,
    max_lsn: u64,
) -> Result<()> {
    let lsn_col = batch
        .column(ncols)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("missing __gdb_lsn column")?;
    for row in 0..batch.num_rows() {
        if let Some((deleted, base)) = visibility {
            if deleted.contains(base + row as u32) {
                continue;
            }
        }
        if lsn_col.value(row) > max_lsn {
            continue;
        }
        let mut vals = Vec::with_capacity(ncols);
        for c in 0..ncols {
            let col = batch.column(c);
            let v: Option<String> = if col.is_null(row) {
                None
            } else {
                match col.data_type() {
                    DataType::Int64 => Some(
                        col.as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("typed")
                            .value(row)
                            .to_string(),
                    ),
                    DataType::Float64 => Some(
                        col.as_any()
                            .downcast_ref::<Float64Array>()
                            .expect("typed")
                            .value(row)
                            .to_string(),
                    ),
                    DataType::Boolean => Some(
                        if col
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .expect("typed")
                            .value(row)
                        {
                            "t".to_string()
                        } else {
                            "f".to_string()
                        },
                    ),
                    _ => Some(
                        col.as_any()
                            .downcast_ref::<StringArray>()
                            .expect("typed")
                            .value(row)
                            .to_string(),
                    ),
                }
            };
            vals.push(v);
        }
        out.push(vals);
    }
    Ok(())
}

/// Split COPY text data into lines (same contract as graydb-check::multiset).
fn graydb_check_split(data: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut start = 0usize;
    for (i, &b) in data.iter().enumerate() {
        if b == b'\n' {
            out.push(&data[start..i]);
            start = i + 1;
        }
    }
    if start < data.len() {
        out.push(&data[start..]);
    }
    out
}
