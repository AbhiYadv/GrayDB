//! Parallel exported-snapshot COPY (wedge spec section 5): N workers import the SAME
//! exported snapshot (valid while the slot-creating replication connection stays open),
//! COPY disjoint ctid ranges, so every staged byte is the database exactly at LSN0.
//! Staging format (D-003): raw COPY text parts per table + manifest.json.
//! Ranges are per-part idempotent restart units; the final range of each table is
//! open-ended so stale relpages statistics can never lose rows.

use crate::config::Config;
use crate::{quote_ident, quote_literal};
use anyhow::{Context, Result};
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio_postgres::Client;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotManifest {
    /// The slot's consistent point: the exact-LSN anchor of this load.
    pub lsn0: String,
    pub snapshot_name: String,
    pub publication: String,
    pub slot: String,
    pub source_schema: String,
    pub taken_at_unix_secs: u64,
    pub tables: Vec<TableManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableManifest {
    /// schema-qualified, e.g. "app.orders"
    pub table: String,
    pub columns: Vec<String>,
    /// pg_type oids, parallel to `columns` (drives the columnar type mapping).
    pub column_oids: Vec<u32>,
    /// Replica identity columns (RI index if set, else PK; empty = append-only).
    pub key_columns: Vec<String>,
    pub rows: u64,
    pub bytes: u64,
    pub parts: Vec<PartManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartManifest {
    /// Path relative to the snapshot directory.
    pub file: String,
    pub rows: u64,
    pub bytes: u64,
    pub first_page: u32,
    /// None = open-ended final range.
    pub end_page: Option<u32>,
}

#[derive(Debug, Clone)]
struct CopyJob {
    table_idx: usize,
    table: String,
    select_cols: String,
    first_page: u32,
    end_page: Option<u32>,
    out_file: PathBuf,
    rel_file: String,
}

#[derive(Debug, Clone)]
struct TablePlan {
    table: String,
    columns: Vec<String>,
    column_oids: Vec<u32>,
    key_columns: Vec<String>,
}

/// Begin a REPEATABLE READ READ ONLY transaction pinned to the exported snapshot.
pub async fn begin_snapshot_txn(client: &Client, snapshot_name: &str) -> Result<()> {
    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await?;
    client
        .batch_execute(&format!(
            "SET TRANSACTION SNAPSHOT {}",
            quote_literal(snapshot_name)
        ))
        .await
        .context("SET TRANSACTION SNAPSHOT (is the replication connection still open?)")?;
    Ok(())
}

/// Run the parallel initial load. Returns the manifest (also written to disk).
pub async fn run_parallel_copy(
    cfg: &Config,
    lsn0: &str,
    snapshot_name: &str,
    snapshot_dir: &Path,
) -> Result<SnapshotManifest> {
    tokio::fs::create_dir_all(snapshot_dir).await?;

    // Planning session, pinned to the snapshot: table list, columns, page counts.
    let planner = cfg.connect().await?;
    begin_snapshot_txn(&planner, snapshot_name).await?;

    let table_rows = planner
        .query(
            "SELECT n.nspname AS schema, c.relname AS name, c.relpages
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relkind = 'r'
             ORDER BY c.relpages DESC",
            &[&cfg.source.schema],
        )
        .await
        .context("listing tables")?;

    let mut plans: Vec<TablePlan> = Vec::new();
    let mut jobs: VecDeque<CopyJob> = VecDeque::new();

    for row in &table_rows {
        let schema: String = row.get("schema");
        let name: String = row.get("name");
        let relpages: i32 = row.get("relpages");
        let qualified = format!("{schema}.{name}");

        let col_rows = planner
            .query(
                "SELECT a.attname, a.atttypid
                 FROM pg_attribute a
                 WHERE a.attrelid = ($1 || '.' || $2)::regclass
                   AND a.attnum > 0 AND NOT a.attisdropped
                 ORDER BY a.attnum",
                &[&quote_ident(&schema), &quote_ident(&name)],
            )
            .await
            .with_context(|| format!("listing columns of {qualified}"))?;
        let columns: Vec<String> = col_rows.iter().map(|r| r.get(0)).collect();
        let column_oids: Vec<u32> = col_rows
            .iter()
            .map(|r| r.get::<_, tokio_postgres::types::Oid>(1))
            .collect();
        anyhow::ensure!(!columns.is_empty(), "{qualified} has no columns");

        // Replica identity columns: explicit RI index wins, else the primary key.
        let key_columns: Vec<String> = {
            let q = "SELECT a.attname
                     FROM pg_index i
                     JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                     WHERE i.indrelid = ($1 || '.' || $2)::regclass AND i.indisreplident
                     ORDER BY a.attnum";
            let ri: Vec<String> = planner
                .query(q, &[&quote_ident(&schema), &quote_ident(&name)])
                .await?
                .iter()
                .map(|r| r.get(0))
                .collect();
            if !ri.is_empty() {
                ri
            } else {
                planner
                    .query(
                        "SELECT a.attname
                         FROM pg_index i
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                         WHERE i.indrelid = ($1 || '.' || $2)::regclass AND i.indisprimary
                         ORDER BY a.attnum",
                        &[&quote_ident(&schema), &quote_ident(&name)],
                    )
                    .await?
                    .iter()
                    .map(|r| r.get(0))
                    .collect()
            }
        };
        let select_cols = columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        let table_idx = plans.len();
        let dir_name = format!("{schema}.{name}");
        tokio::fs::create_dir_all(snapshot_dir.join(&dir_name)).await?;

        // Page ranges: aim for one part per stream; final range always open-ended.
        let streams = cfg.initial_load.copy_streams.max(1) as u32;
        let relpages = relpages.max(0) as u32;
        let chunk = (relpages / streams).max(1);
        let mut boundaries: Vec<u32> = Vec::new();
        let mut p = 0u32;
        while p < relpages && boundaries.len() + 1 < streams as usize {
            boundaries.push(p);
            p += chunk;
        }
        boundaries.push(p); // final, open-ended start
        for (part, window) in boundaries.windows(2).enumerate() {
            jobs.push_back(make_job(
                table_idx, &qualified, &select_cols, window[0], Some(window[1]),
                part, snapshot_dir, &dir_name,
            ));
        }
        let last_start = *boundaries.last().expect("nonempty");
        jobs.push_back(make_job(
            table_idx, &qualified, &select_cols, last_start, None,
            boundaries.len() - 1, snapshot_dir, &dir_name,
        ));

        plans.push(TablePlan {
            table: qualified,
            columns,
            column_oids,
            key_columns,
        });
    }
    planner.batch_execute("COMMIT").await.ok();
    drop(planner);

    tracing::info!(
        tables = plans.len(),
        parts = jobs.len(),
        streams = cfg.initial_load.copy_streams,
        "parallel COPY plan ready"
    );

    // Worker pool: each worker pins its own session to the same exported snapshot.
    let queue = Arc::new(Mutex::new(jobs));
    let mut handles = Vec::new();
    for worker_id in 0..cfg.initial_load.copy_streams.max(1) {
        let queue = Arc::clone(&queue);
        let cfg = cfg.clone();
        let snapshot_name = snapshot_name.to_string();
        handles.push(tokio::spawn(async move {
            copy_worker(worker_id, cfg, snapshot_name, queue).await
        }));
    }

    let mut part_results: Vec<(usize, PartManifest)> = Vec::new();
    for h in handles {
        let worker_parts = h.await.context("copy worker panicked")??;
        part_results.extend(worker_parts);
    }

    // Assemble manifest.
    let mut tables: Vec<TableManifest> = plans
        .iter()
        .map(|p| TableManifest {
            table: p.table.clone(),
            columns: p.columns.clone(),
            column_oids: p.column_oids.clone(),
            key_columns: p.key_columns.clone(),
            rows: 0,
            bytes: 0,
            parts: Vec::new(),
        })
        .collect();
    for (table_idx, part) in part_results {
        let t = &mut tables[table_idx];
        t.rows += part.rows;
        t.bytes += part.bytes;
        t.parts.push(part);
    }
    for t in &mut tables {
        t.parts.sort_by_key(|p| p.first_page);
    }

    let manifest = SnapshotManifest {
        lsn0: lsn0.to_string(),
        snapshot_name: snapshot_name.to_string(),
        publication: cfg.source.publication.clone(),
        slot: cfg.source.slot.clone(),
        source_schema: cfg.source.schema.clone(),
        taken_at_unix_secs: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        tables,
    };
    let manifest_path = snapshot_dir.join("manifest.json");
    tokio::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?).await?;
    tracing::info!(manifest = %manifest_path.display(), lsn0, "snapshot staged");
    Ok(manifest)
}

fn make_job(
    table_idx: usize,
    qualified: &str,
    select_cols: &str,
    first_page: u32,
    end_page: Option<u32>,
    part: usize,
    snapshot_dir: &Path,
    dir_name: &str,
) -> CopyJob {
    let rel_file = format!("{dir_name}/part-{part:04}.copy");
    CopyJob {
        table_idx,
        table: qualified.to_string(),
        select_cols: select_cols.to_string(),
        first_page,
        end_page,
        out_file: snapshot_dir.join(&rel_file),
        rel_file,
    }
}

async fn copy_worker(
    worker_id: usize,
    cfg: Config,
    snapshot_name: String,
    queue: Arc<Mutex<VecDeque<CopyJob>>>,
) -> Result<Vec<(usize, PartManifest)>> {
    let client = cfg.connect().await?;
    begin_snapshot_txn(&client, &snapshot_name).await?;
    let mut out = Vec::new();
    loop {
        let job = { queue.lock().await.pop_front() };
        let Some(job) = job else { break };

        let (schema, name) = job
            .table
            .split_once('.')
            .context("qualified table name")?;
        let range_pred = match job.end_page {
            Some(end) => format!(
                "WHERE ctid >= '({},0)'::tid AND ctid < '({},0)'::tid",
                job.first_page, end
            ),
            None => format!("WHERE ctid >= '({},0)'::tid", job.first_page),
        };
        let sql = format!(
            "COPY (SELECT {} FROM {}.{} {}) TO STDOUT",
            job.select_cols,
            quote_ident(schema),
            quote_ident(name),
            range_pred
        );

        let mut file = tokio::fs::File::create(&job.out_file)
            .await
            .with_context(|| format!("creating {}", job.out_file.display()))?;
        let stream = client
            .copy_out(&sql)
            .await
            .with_context(|| format!("COPY out {} part {}", job.table, job.rel_file))?;
        futures_util::pin_mut!(stream);

        let mut rows = 0u64;
        let mut bytes = 0u64;
        while let Some(chunk) = stream.try_next().await? {
            rows += chunk.iter().filter(|&&b| b == b'\n').count() as u64;
            bytes += chunk.len() as u64;
            file.write_all(&chunk).await?;
        }
        file.flush().await?;

        tracing::debug!(worker_id, table = %job.table, part = %job.rel_file, rows, bytes, "part staged");
        out.push((
            job.table_idx,
            PartManifest {
                file: job.rel_file,
                rows,
                bytes,
                first_page: job.first_page,
                end_page: job.end_page,
            },
        ));
    }
    client.batch_execute("COMMIT").await.ok();
    Ok(out)
}
