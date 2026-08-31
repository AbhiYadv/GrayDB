//! LsnTableProvider: the real read path (R1 prerequisite P3, T5's measured shape —
//! "the visibility predicate evaluated inside our scans").
//!
//! Instead of copying every visible row into a MemTable per query, this streams
//! parquet segments batch-by-batch with:
//!   - segment pruning  (skip whole segments where lsn_min > target),
//!   - projection pushdown into parquet (only requested columns are decoded;
//!     `__gdb_lsn` is always read internally for masking),
//!   - per-batch row masking (insert_lsn <= L && !deleted_by(<= L), delete sidecar
//!     turned into a roaring bitmap once per segment),
//!   - a memtable overlay: the store's not-yet-flushed open rows, pre-filtered at
//!     snapshot time, unioned in as the final partition (R1/P2 — freshness without
//!     flushing tiny segments per tick).
//!
//! Memory stays bounded by batch size; latency scales with bytes actually needed.

use arrow::array::{BooleanArray, UInt64Array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use graydb_columnar::SegmentSnapshot;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use roaring::RoaringBitmap;

use std::fmt;
use std::sync::Arc;

/// A point-in-time, lock-free view of one table: immutable segment files + their
/// delete states + the open-row overlay, all pinned to `target_lsn`.
#[derive(Debug)]
pub struct TableSnapshot {
    pub name: String,
    /// Full table schema INCLUDING the trailing `__gdb_lsn` column.
    pub schema: SchemaRef,
    pub segments: Vec<SegmentSnapshot>,
    /// Open rows visible at target_lsn (already filtered), full schema.
    pub overlay: Option<RecordBatch>,
    pub target_lsn: u64,
    /// The shape's applied LSN at snapshot time (for the LSN proof).
    pub applied_lsn: u64,
}

#[derive(Debug)]
pub struct LsnTableProvider {
    snap: Arc<TableSnapshot>,
}

impl LsnTableProvider {
    pub fn new(snap: Arc<TableSnapshot>) -> Self {
        LsnTableProvider { snap }
    }
}

#[async_trait]
impl TableProvider for LsnTableProvider {
    fn schema(&self) -> SchemaRef {
        self.snap.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // We don't evaluate SQL filters ourselves (yet — row-group pruning is a later
        // optimization); Inexact makes DataFusion re-apply them above the scan.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let proj: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..self.snap.schema.fields().len()).collect(),
        };
        let out_schema = Arc::new(self.snap.schema.project(&proj)?);
        Ok(Arc::new(LsnScanExec::new(
            Arc::clone(&self.snap),
            proj,
            out_schema,
        )))
    }
}

pub struct LsnScanExec {
    snap: Arc<TableSnapshot>,
    proj: Vec<usize>,
    out_schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl LsnScanExec {
    fn new(snap: Arc<TableSnapshot>, proj: Vec<usize>, out_schema: SchemaRef) -> Self {
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(out_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        LsnScanExec {
            snap,
            proj,
            out_schema,
            props,
        }
    }
}

impl fmt::Debug for LsnScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LsnScanExec({} @{}, {} segments)",
            self.snap.name,
            self.snap.target_lsn,
            self.snap.segments.len()
        )
    }
}

impl DisplayAs for LsnScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LsnScanExec: table={} target_lsn={} segments={} overlay_rows={}",
            self.snap.name,
            self.snap.target_lsn,
            self.snap.segments.len(),
            self.snap.overlay.as_ref().map(|b| b.num_rows()).unwrap_or(0)
        )
    }
}

impl ExecutionPlan for LsnScanExec {
    fn name(&self) -> &'static str {
        "LsnScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let iter = SegmentIter::new(
            Arc::clone(&self.snap),
            self.proj.clone(),
            self.out_schema.clone(),
        );
        let stream = futures_util::stream::iter(iter);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.out_schema.clone(),
            stream,
        )))
    }
}

/// Lazy iterator: opens one segment at a time, yields masked+projected batches,
/// finishes with the overlay. Sync parquet IO inside an async stream is acceptable
/// for R1-local (page-cache reads); a spawn_blocking bridge is a later optimization.
struct SegmentIter {
    snap: Arc<TableSnapshot>,
    proj: Vec<usize>,
    out_schema: SchemaRef,
    /// Sorted unique column indices actually read from parquet (proj + lsn column).
    read_set: Vec<usize>,
    /// Position of each output column within read_set order.
    out_positions: Vec<usize>,
    /// Position of __gdb_lsn within read_set order.
    lsn_position: usize,
    seg_idx: usize,
    reader: Option<ParquetRecordBatchReader>,
    deleted: RoaringBitmap,
    base: u32,
    overlay_done: bool,
}

impl SegmentIter {
    fn new(snap: Arc<TableSnapshot>, proj: Vec<usize>, out_schema: SchemaRef) -> Self {
        let lsn_idx = snap.schema.fields().len() - 1; // __gdb_lsn is always last
        let mut read_set: Vec<usize> = proj.clone();
        if !read_set.contains(&lsn_idx) {
            read_set.push(lsn_idx);
        }
        read_set.sort_unstable();
        read_set.dedup();
        let out_positions = proj
            .iter()
            .map(|p| read_set.iter().position(|r| r == p).expect("in read_set"))
            .collect();
        let lsn_position = read_set.iter().position(|r| *r == lsn_idx).expect("lsn");
        SegmentIter {
            snap,
            proj,
            out_schema,
            read_set,
            out_positions,
            lsn_position,
            seg_idx: 0,
            reader: None,
            deleted: RoaringBitmap::new(),
            base: 0,
            overlay_done: false,
        }
    }

    fn open_next_segment(&mut self) -> DfResult<bool> {
        while self.seg_idx < self.snap.segments.len() {
            let seg = &self.snap.segments[self.seg_idx];
            self.seg_idx += 1;
            if seg.meta.lsn_min > self.snap.target_lsn {
                continue; // pruned: nothing in this segment can be visible
            }
            self.deleted = seg
                .deletes
                .iter()
                .filter(|(_, dlsn)| *dlsn <= self.snap.target_lsn)
                .map(|(row, _)| *row)
                .collect();
            self.base = 0;
            let file = std::fs::File::open(&seg.path)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let mask = ProjectionMask::roots(builder.parquet_schema(), self.read_set.clone());
            let reader = builder
                .with_projection(mask)
                .build()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            self.reader = Some(reader);
            return Ok(true);
        }
        Ok(false)
    }

    /// Mask a read batch (columns in read_set order) and project to output order.
    fn mask_and_project(&mut self, batch: RecordBatch) -> DfResult<Option<RecordBatch>> {
        let n = batch.num_rows();
        let lsn_col = batch
            .column(self.lsn_position)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| DataFusionError::Internal("__gdb_lsn not u64".into()))?;
        let base = self.base;
        self.base += n as u32;
        let mask: BooleanArray = (0..n)
            .map(|i| {
                Some(
                    lsn_col.value(i) <= self.snap.target_lsn
                        && !self.deleted.contains(base + i as u32),
                )
            })
            .collect();
        let filtered = filter_record_batch(&batch, &mask)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        if filtered.num_rows() == 0 {
            return Ok(None);
        }
        let cols = self
            .out_positions
            .iter()
            .map(|&p| filtered.column(p).clone())
            .collect::<Vec<_>>();
        let out = RecordBatch::try_new_with_options(
            self.out_schema.clone(),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(filtered.num_rows())),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        Ok(Some(out))
    }

    fn project_overlay(&self, batch: &RecordBatch) -> DfResult<RecordBatch> {
        // Overlay carries the FULL schema; visibility was applied at snapshot time.
        let cols = self
            .proj
            .iter()
            .map(|&p| batch.column(p).clone())
            .collect::<Vec<_>>();
        RecordBatch::try_new_with_options(
            self.out_schema.clone(),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl Iterator for SegmentIter {
    type Item = DfResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(reader) = self.reader.as_mut() {
                match reader.next() {
                    Some(Ok(batch)) => match self.mask_and_project(batch) {
                        Ok(Some(out)) => return Some(Ok(out)),
                        Ok(None) => continue,
                        Err(e) => return Some(Err(e)),
                    },
                    Some(Err(e)) => {
                        return Some(Err(DataFusionError::External(Box::new(e))))
                    }
                    None => {
                        self.reader = None;
                        continue;
                    }
                }
            }
            match self.open_next_segment() {
                Ok(true) => continue,
                Ok(false) => {
                    if self.overlay_done {
                        return None;
                    }
                    self.overlay_done = true;
                    if let Some(overlay) = self.snap.overlay.clone() {
                        if overlay.num_rows() > 0 {
                            return Some(self.project_overlay(&overlay));
                        }
                    }
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}
