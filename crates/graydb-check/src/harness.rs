//! Shared demo-driver helpers: source-truth multisets and store projections.

use anyhow::Result;
use futures_util::TryStreamExt;
use graydb_columnar::copytext;

/// Current source rows for `table` (selected columns) as a sorted multiset of
/// tab-joined raw values (COPY text unescaped, so both sides are raw renderings).
pub async fn source_multiset(
    admin: &tokio_postgres::Client,
    table: &str,
    cols: &str,
) -> Result<Vec<String>> {
    let sql = format!("COPY (SELECT {cols} FROM {table}) TO STDOUT");
    let stream = admin.copy_out(&sql).await?;
    futures_util::pin_mut!(stream);
    let mut data: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.try_next().await? {
        data.extend_from_slice(&chunk);
    }
    let mut out: Vec<String> = Vec::new();
    for line in data.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let vals = copytext::parse_copy_line(line);
        out.push(join_vals(&vals.iter().map(|v| v.as_deref()).collect::<Vec<_>>()));
    }
    out.sort_unstable();
    Ok(out)
}

/// Project store scan rows onto `cols` indices as a sorted multiset.
pub fn project_multiset(rows: Vec<Vec<Option<String>>>, cols: &[usize]) -> Vec<String> {
    let mut out: Vec<String> = rows
        .iter()
        .map(|r| {
            let picked: Vec<Option<&str>> = cols.iter().map(|&i| r[i].as_deref()).collect();
            join_vals(&picked)
        })
        .collect();
    out.sort_unstable();
    out
}

pub fn join_vals(vals: &[Option<&str>]) -> String {
    vals.iter()
        .map(|v| v.unwrap_or("\u{0}NULL"))
        .collect::<Vec<_>>()
        .join("\t")
}
