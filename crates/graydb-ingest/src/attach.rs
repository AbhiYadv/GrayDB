//! Attach = install the SQL-objects-only footprint (I5) and report table eligibility
//! (Amendment A section A5): PK/replica identity for update/delete tables,
//! REPLICA IDENTITY NOTHING => append-only, surfaced at attach — never silently.

use crate::{quote_ident, quote_literal};
use anyhow::{Context, Result};
use tokio_postgres::Client;

pub const ATTACH_PACK_SQL: &str = include_str!("../sql/attach_pack.sql");

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Eligibility {
    /// PK or replica identity index: full update/delete eligibility.
    Full,
    /// REPLICA IDENTITY FULL: eligible, with documented WAL-inflation warning.
    FullIdentityWarning,
    /// REPLICA IDENTITY NOTHING / no identity: append-only.
    AppendOnly,
}

impl std::fmt::Display for Eligibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Eligibility::Full => write!(f, "full"),
            Eligibility::FullIdentityWarning => write!(f, "full (RI FULL: WAL-inflation warning)"),
            Eligibility::AppendOnly => write!(f, "append-only"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableEligibility {
    pub table: String, // schema-qualified
    pub replident: i8, // pg_class.relreplident as raw char
    pub has_pk: bool,
    pub eligibility: Eligibility,
}

/// Run the idempotent attach pack (schema graydb + ddl_log + event triggers).
pub async fn install_attach_pack(client: &Client) -> Result<()> {
    client
        .batch_execute(ATTACH_PACK_SQL)
        .await
        .context("installing graydb attach pack")
}

/// Create the publication for the target schema plus graydb.ddl_log (in-stream DDL).
/// Idempotent: no-op when the publication already exists.
pub async fn ensure_publication(client: &Client, publication: &str, schema: &str) -> Result<()> {
    let exists = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?
        .is_some();
    if exists {
        tracing::info!(publication, "publication already exists");
        return Ok(());
    }
    let sql = format!(
        "CREATE PUBLICATION {} FOR TABLES IN SCHEMA {}, TABLE graydb.ddl_log",
        quote_ident(publication),
        quote_ident(schema)
    );
    client.batch_execute(&sql).await.context("creating publication")?;
    tracing::info!(publication, schema, "publication created");
    Ok(())
}

pub async fn drop_publication_if_exists(client: &Client, publication: &str) -> Result<()> {
    let sql = format!("DROP PUBLICATION IF EXISTS {}", quote_ident(publication));
    client.batch_execute(&sql).await?;
    Ok(())
}

/// Drop an inactive slot left over from a previous run (fresh demo start).
pub async fn drop_slot_if_exists(client: &Client, slot: &str) -> Result<()> {
    let row = client
        .query_opt(
            "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await?;
    if let Some(row) = row {
        let active: bool = row.get(0);
        anyhow::ensure!(!active, "slot {slot} is active; refusing to drop");
        client
            .execute(
                &format!("SELECT pg_drop_replication_slot({})", quote_literal(slot)),
                &[],
            )
            .await
            .context("dropping stale replication slot")?;
        tracing::info!(slot, "stale slot dropped");
    }
    Ok(())
}

/// Eligibility scan for every ordinary table in the schema (Amendment A A5.1).
pub async fn eligibility_scan(client: &Client, schema: &str) -> Result<Vec<TableEligibility>> {
    let rows = client
        .query(
            "SELECT n.nspname || '.' || c.relname AS table,
                    c.relreplident::text AS replident,
                    EXISTS (SELECT 1 FROM pg_index i
                            WHERE i.indrelid = c.oid AND i.indisprimary) AS has_pk,
                    EXISTS (SELECT 1 FROM pg_index i
                            WHERE i.indrelid = c.oid AND i.indisreplident) AS has_ri_index
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relkind = 'r'
             ORDER BY 1",
            &[&schema],
        )
        .await
        .context("eligibility scan")?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let table: String = row.get("table");
        let replident_s: String = row.get("replident");
        let replident = replident_s.bytes().next().unwrap_or(b'd') as i8;
        let has_pk: bool = row.get("has_pk");
        let has_ri_index: bool = row.get("has_ri_index");
        let eligibility = match replident as u8 {
            b'f' => Eligibility::FullIdentityWarning,
            b'n' => Eligibility::AppendOnly,
            b'i' if has_ri_index => Eligibility::Full,
            b'd' if has_pk => Eligibility::Full,
            // default identity without a PK behaves like no identity for update/delete
            _ => Eligibility::AppendOnly,
        };
        out.push(TableEligibility {
            table,
            replident,
            has_pk,
            eligibility,
        });
    }
    Ok(out)
}
