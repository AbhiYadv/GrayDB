//! SP1 check: exact row-multiset equality at LSN0 (Decision D-004).
//! Both sides render through the source's own COPY text format — the Type
//! Interpretation Contract v0 — so equality is semantic equality at LSN0:
//!   source side: full-table COPY inside a transaction pinned to the SAME exported
//!                snapshot the load used;
//!   staged side: the union of the parallel ctid-range part files.
//! Multiset compare = sort all rows as byte lines, SHA-256 the sorted stream + count.
//! Named limitation: in-memory sort; fine at demo scale, untested at multi-GB tables.

use anyhow::{Context, Result};
use futures_util::TryStreamExt;
use graydb_ingest::quote_ident;
use graydb_ingest::snapshot::TableManifest;
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio_postgres::Client;

#[derive(Debug, Clone)]
pub struct TableCheck {
    pub table: String,
    pub source_rows: u64,
    pub staged_rows: u64,
    pub source_hash: String,
    pub staged_hash: String,
    pub pass: bool,
}

/// Split COPY text output into rows. Raw 0x0A only occurs as the row terminator
/// (COPY escapes newlines inside values), so splitting on '\n' is exact. The final
/// segment after a trailing newline is not a row; a genuinely empty line IS a row
/// (single empty text column), so we must not blanket-filter empties.
pub fn split_copy_lines(data: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut start = 0usize;
    for (i, &b) in data.iter().enumerate() {
        if b == b'\n' {
            out.push(&data[start..i]);
            start = i + 1;
        }
    }
    if start < data.len() {
        // COPY output always ends with '\n'; a trailing partial line means truncation.
        out.push(&data[start..]);
    }
    out
}

fn sorted_multiset_hash(mut lines: Vec<Vec<u8>>) -> (u64, String) {
    lines.sort_unstable();
    let mut hasher = Sha256::new();
    for line in &lines {
        hasher.update(line);
        hasher.update(b"\n");
    }
    (lines.len() as u64, format!("{:x}", hasher.finalize()))
}

/// Check one table: COPY the full table from the pinned-snapshot session and compare
/// against the staged parts. `client` must already be inside the snapshot transaction
/// (graydb_ingest::snapshot::begin_snapshot_txn).
pub async fn check_table_at_snapshot(
    client: &Client,
    manifest: &TableManifest,
    snapshot_dir: &Path,
) -> Result<TableCheck> {
    // Source side, at LSN0 by construction.
    let (schema, name) = manifest
        .table
        .split_once('.')
        .context("qualified table name")?;
    let select_cols = manifest
        .columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "COPY (SELECT {} FROM {}.{}) TO STDOUT",
        select_cols,
        quote_ident(schema),
        quote_ident(name)
    );
    let stream = client.copy_out(&sql).await?;
    futures_util::pin_mut!(stream);
    let mut source_data: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.try_next().await? {
        source_data.extend_from_slice(&chunk);
    }
    let source_lines: Vec<Vec<u8>> = split_copy_lines(&source_data)
        .into_iter()
        .map(|l| l.to_vec())
        .collect();
    drop(source_data);
    let (source_rows, source_hash) = sorted_multiset_hash(source_lines);

    // Staged side: union of the ctid-range parts.
    let mut staged_lines: Vec<Vec<u8>> = Vec::new();
    for part in &manifest.parts {
        let path = snapshot_dir.join(&part.file);
        let data = tokio::fs::read(&path)
            .await
            .with_context(|| format!("reading staged part {}", path.display()))?;
        staged_lines.extend(split_copy_lines(&data).into_iter().map(|l| l.to_vec()));
    }
    let (staged_rows, staged_hash) = sorted_multiset_hash(staged_lines);

    let pass = source_rows == staged_rows
        && source_hash == staged_hash
        && staged_rows == manifest.rows;
    Ok(TableCheck {
        table: manifest.table.clone(),
        source_rows,
        staged_rows,
        source_hash,
        staged_hash,
        pass,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_keeps_empty_rows_and_drops_trailing_terminator() {
        // three rows: "a\tb", "" (legit empty single-col row), "c"
        let data = b"a\tb\n\nc\n";
        let lines = split_copy_lines(data);
        assert_eq!(lines, vec![&b"a\tb"[..], &b""[..], &b"c"[..]]);
    }

    #[test]
    fn multiset_hash_is_order_independent() {
        let a = vec![b"x".to_vec(), b"y".to_vec(), b"".to_vec()];
        let b = vec![b"y".to_vec(), b"".to_vec(), b"x".to_vec()];
        assert_eq!(sorted_multiset_hash(a), sorted_multiset_hash(b));
    }

    #[test]
    fn multiset_hash_counts_duplicates() {
        let a = vec![b"x".to_vec(), b"x".to_vec()];
        let b = vec![b"x".to_vec()];
        assert_ne!(sorted_multiset_hash(a), sorted_multiset_hash(b));
    }
}
