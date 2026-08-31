//! Disk-side reader: typed RecordBatches with LSN visibility applied, built purely
//! from on-disk artifacts (manifest.json + parquet segments + delete sidecars).
//! This is the SP6 reader's data path — deliberately independent of the live
//! TableStore so a reader process needs nothing but the directory (I3).

use anyhow::{Context, Result};
use arrow::array::{BooleanArray, UInt64Array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use roaring::RoaringBitmap;
use std::path::Path;

use crate::store::{arrow_schema_for, SegmentSnapshot, StoreManifest};

/// Load the manifest of a finalized table store.
pub fn read_manifest(dir: &Path) -> Result<StoreManifest> {
    let raw = std::fs::read(dir.join("manifest.json"))
        .with_context(|| format!("reading manifest in {}", dir.display()))?;
    Ok(serde_json::from_slice(&raw)?)
}

/// Parse one segment's delete sidecar ((row, delete_lsn) pairs); absent file = none.
pub fn read_sidecar(dir: &Path, seg_id: u32) -> Result<Vec<(u32, u64)>> {
    let path = dir.join(format!("seg-{seg_id:06}.del.json"));
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = std::fs::read(&path)?;
    let sidecar: serde_json::Value = serde_json::from_slice(&raw)?;
    let mut out = Vec::new();
    if let Some(pairs) = sidecar.get("deletes").and_then(|d| d.as_array()) {
        for pair in pairs {
            if let (Some(row), Some(dlsn)) = (
                pair.get(0).and_then(|v| v.as_u64()),
                pair.get(1).and_then(|v| v.as_u64()),
            ) {
                out.push((row as u32, dlsn));
            }
        }
    }
    Ok(out)
}

/// Disk-side snapshot for the streaming reader: schema + per-segment views + the
/// store's applied LSN, from a FINALIZED directory (manifest + parquet + sidecars).
pub fn load_segment_snapshots(
    dir: &Path,
) -> Result<(arrow::datatypes::SchemaRef, Vec<SegmentSnapshot>, u64)> {
    let manifest = read_manifest(dir)?;
    let specs: Vec<crate::ColumnSpec> = manifest.columns.clone();
    let schema = arrow_schema_for(&specs);
    let mut segments = Vec::with_capacity(manifest.segments.len());
    for meta in &manifest.segments {
        segments.push(SegmentSnapshot {
            meta: meta.clone(),
            path: dir.join(format!("seg-{:06}.parquet", meta.id)),
            deletes: read_sidecar(dir, meta.id)?,
        });
    }
    Ok((schema, segments, manifest.applied_lsn))
}

/// All rows visible at `lsn` as typed batches (the trailing `__gdb_lsn` column is
/// kept — it is the per-row proof the reader surfaces).
pub fn read_visible_batches(dir: &Path, lsn: u64) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let manifest = read_manifest(dir)?;
    let mut schema: Option<SchemaRef> = None;
    let mut out: Vec<RecordBatch> = Vec::new();

    for seg in &manifest.segments {
        if seg.lsn_min > lsn {
            continue;
        }
        // Deletes at-or-before lsn for this segment.
        let mut deleted = RoaringBitmap::new();
        let sidecar_path = dir.join(format!("seg-{:06}.del.json", seg.id));
        if sidecar_path.exists() {
            let raw = std::fs::read(&sidecar_path)?;
            let sidecar: serde_json::Value = serde_json::from_slice(&raw)?;
            if let Some(pairs) = sidecar.get("deletes").and_then(|d| d.as_array()) {
                for pair in pairs {
                    let (Some(row), Some(dlsn)) = (
                        pair.get(0).and_then(|v| v.as_u64()),
                        pair.get(1).and_then(|v| v.as_u64()),
                    ) else {
                        continue;
                    };
                    if dlsn <= lsn {
                        deleted.insert(row as u32);
                    }
                }
            }
        }

        let file = std::fs::File::open(dir.join(format!("seg-{:06}.parquet", seg.id)))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let mut base = 0u32;
        for batch in reader {
            let batch = batch?;
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            let lsn_col = batch
                .column(batch.num_columns() - 1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("missing __gdb_lsn column")?;
            let mask: BooleanArray = (0..batch.num_rows())
                .map(|row| {
                    Some(!deleted.contains(base + row as u32) && lsn_col.value(row) <= lsn)
                })
                .collect();
            let filtered = filter_record_batch(&batch, &mask)?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
            base += batch.num_rows() as u32;
        }
    }
    let schema = match schema {
        Some(s) => s,
        // Table with zero visible segments: derive schema from the manifest columns
        // is possible, but no demo path needs it yet — fail loudly instead of guessing.
        None => anyhow::bail!("{}: no segments readable at {lsn}", manifest.table),
    };
    Ok((schema, out))
}
