//! graydb-studio reader (SP6a): SQL over both shapes via DataFusion, every query at
//! a caller-supplied target LSN, plus the `search(table, query)` table function and
//! the `graydb.stat_replication` view (pg_stat_replication-style, D-014 naming).
//! Every result carries an LsnProof — the exact source LSN it reflects (I4).
//! The axum server + HTML page land in SP8; this library is the engine they serve.

pub mod engine;
pub mod provider;

use crate::provider::{LsnTableProvider, TableSnapshot};
use anyhow::{Context, Result};
use arrow::array::{Float32Array, Int64Array, StringArray};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{MemorySchemaProvider, TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
// (columnar reads now flow through provider.rs streaming scans)
use graydb_ingest::repl::format_lsn;
use graydb_search::SearchReader;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TableShape {
    /// Qualified name, e.g. "app.orders".
    pub name: String,
    pub dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LsnProof {
    pub target_lsn: Option<u64>,
    /// Last source WAL position received on the replication stream
    /// (pg_stat_subscription.received_lsn semantics, D-014).
    pub received_lsn: u64,
    /// (shape, applied_lsn) per registered shape.
    pub shapes: Vec<(String, u64)>,
}

impl LsnProof {
    pub fn render(&self) -> String {
        let target = match self.target_lsn {
            Some(l) => format_lsn(l),
            None => "head".to_string(),
        };
        let shapes = self
            .shapes
            .iter()
            .map(|(s, l)| format!("{s}@{}", format_lsn(*l)))
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "LSN proof: target={target} received={} {shapes}",
            format_lsn(self.received_lsn)
        )
    }
}

pub struct Reader {
    tables: Vec<TableShape>,
    search: HashMap<String, Arc<SearchReader>>,
    pub received_lsn: u64,
}

impl Reader {
    /// Open from on-disk artifacts only (I3: a reader needs nothing but directories).
    pub fn open(
        tables: Vec<TableShape>,
        search_dirs: Vec<(String, PathBuf)>,
        received_lsn: u64,
    ) -> Result<Self> {
        let mut search = HashMap::new();
        for (table, dir) in search_dirs {
            search.insert(table.clone(), Arc::new(SearchReader::open(&dir)?));
        }
        Ok(Reader {
            tables,
            search,
            received_lsn,
        })
    }

    /// Run SQL with every columnar table pinned to `target_lsn` (None = head).
    /// Builds disk-side snapshots (finalized directories) and streams them (P3).
    pub async fn query(
        &self,
        sql: &str,
        target_lsn: Option<u64>,
    ) -> Result<(Vec<RecordBatch>, LsnProof)> {
        let lsn = target_lsn.unwrap_or(u64::MAX);
        let mut snapshots = Vec::with_capacity(self.tables.len());
        for t in &self.tables {
            let (schema, segments, applied_lsn) =
                graydb_columnar::reader::load_segment_snapshots(&t.dir)?;
            snapshots.push(Arc::new(TableSnapshot {
                name: t.name.clone(),
                schema,
                segments,
                overlay: None, // finalized stores have no open rows
                target_lsn: lsn,
                applied_lsn,
            }));
        }
        run_query(snapshots, &self.search, self.received_lsn, sql, target_lsn).await
    }
}

/// The one query path (live engine and disk Reader both come through here):
/// register every table as a streaming LSN scan, plus graydb.stat_replication and
/// the search() table function, run the SQL, return batches + the LSN proof.
pub async fn run_query(
    snapshots: Vec<Arc<TableSnapshot>>,
    search: &HashMap<String, Arc<SearchReader>>,
    received_lsn: u64,
    sql: &str,
    target_lsn: Option<u64>,
) -> Result<(Vec<RecordBatch>, LsnProof)> {
    let ctx = SessionContext::new();
    let catalog = ctx
        .catalog("datafusion")
        .context("default catalog missing")?;
    catalog
        .register_schema("app", Arc::new(MemorySchemaProvider::new()))
        .map_err(anyhow_df)?;
    catalog
        .register_schema("graydb", Arc::new(MemorySchemaProvider::new()))
        .map_err(anyhow_df)?;

    let mut shapes: Vec<(String, u64)> = Vec::new();
    for snap in snapshots {
        shapes.push((format!("columnar:{}", snap.name), snap.applied_lsn));
        ctx.register_table(
            snap.name.clone().as_str(),
            Arc::new(LsnTableProvider::new(snap)),
        )
        .map_err(anyhow_df)?;
    }
    for (table, reader) in search {
        shapes.push((format!("search:{table}"), reader.meta.applied_lsn));
    }

    let stat = stat_replication_batch(received_lsn, &shapes)?;
    ctx.register_table(
        "graydb.stat_replication",
        Arc::new(MemTable::try_new(stat.schema(), vec![vec![stat]]).map_err(anyhow_df)?),
    )
    .map_err(anyhow_df)?;

    ctx.register_udtf(
        "search",
        Arc::new(SearchUdtf {
            readers: search.clone(),
        }),
    );

    let df = ctx.sql(sql).await.map_err(anyhow_df)?;
    let batches = df.collect().await.map_err(anyhow_df)?;
    Ok((
        batches,
        LsnProof {
            target_lsn,
            received_lsn,
            shapes,
        },
    ))
}

fn anyhow_df(e: DataFusionError) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

/// Render batches as (column names, rows of display strings) for the results grid.
pub fn batches_to_rows(batches: &[RecordBatch]) -> (Vec<String>, Vec<Vec<String>>) {
    let mut cols: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::new();
    let opts = FormatOptions::default().with_null("NULL");
    for b in batches {
        if cols.is_empty() {
            cols = b.schema().fields().iter().map(|f| f.name().clone()).collect();
        }
        let formatters: Vec<ArrayFormatter> = match b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts))
            .collect::<std::result::Result<Vec<_>, _>>()
        {
            Ok(f) => f,
            Err(_) => continue,
        };
        for r in 0..b.num_rows() {
            rows.push(formatters.iter().map(|f| f.value(r).to_string()).collect());
        }
    }
    (cols, rows)
}

/// graydb.stat_replication (D-014): shape, received_lsn, applied_lsn, apply_lag_bytes.
fn stat_replication_batch(received: u64, shapes: &[(String, u64)]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("shape", DataType::Utf8, false),
        Field::new("received_lsn", DataType::Utf8, false),
        Field::new("applied_lsn", DataType::Utf8, false),
        Field::new("apply_lag_bytes", DataType::Int64, false),
    ]));
    let shape_col: StringArray = shapes.iter().map(|(s, _)| Some(s.as_str())).collect();
    let received_col: StringArray = shapes
        .iter()
        .map(|_| Some(format_lsn(received)))
        .collect::<Vec<_>>()
        .into();
    let applied_col: StringArray = shapes
        .iter()
        .map(|(_, l)| Some(format_lsn(*l)))
        .collect::<Vec<_>>()
        .into();
    let lag_col: Int64Array = shapes
        .iter()
        .map(|(_, l)| Some(received.saturating_sub(*l) as i64))
        .collect();
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(shape_col),
            Arc::new(received_col),
            Arc::new(applied_col),
            Arc::new(lag_col),
        ],
    )?)
}

/// `search('app.customers', 'query')` table function: (key, score, applied_lsn).
struct SearchUdtf {
    readers: HashMap<String, Arc<SearchReader>>,
}

impl std::fmt::Debug for SearchUdtf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SearchUdtf({} indexes)", self.readers.len())
    }
}

impl TableFunctionImpl for SearchUdtf {
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let lit = |i: usize| -> datafusion::error::Result<String> {
            match args.get(i) {
                Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => Ok(s.clone()),
                _ => Err(DataFusionError::Plan(
                    "search(table, query) expects two string literals".into(),
                )),
            }
        };
        let table = lit(0)?;
        let query = lit(1)?;
        let reader = self.readers.get(&table).ok_or_else(|| {
            DataFusionError::Plan(format!("no search index declared for {table}"))
        })?;
        let hits = reader
            .search(&query, 10_000)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("applied_lsn", DataType::Utf8, false),
        ]));
        let keys: StringArray = hits.iter().map(|(k, _)| Some(k.as_str())).collect();
        let scores: Float32Array = hits.iter().map(|(_, s)| Some(*s)).collect();
        let lsns: StringArray = hits
            .iter()
            .map(|_| Some(format_lsn(reader.meta.applied_lsn)))
            .collect::<Vec<_>>()
            .into();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(keys), Arc::new(scores), Arc::new(lsns)],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}
