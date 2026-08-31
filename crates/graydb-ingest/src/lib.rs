//! graydb-ingest: source attach + replication consumption.
//! Owns: publication/event-trigger pack (SQL, idempotent), slot lifecycle with exported
//! snapshot, parallel ctid-range COPY at LSN0, pgoutput frame consumption, WAL-budget
//! ladder (warn -> shed -> spill -> fresh-session raw capture -> deliberate slot drop).
//! LAW: ack only what graydb-log has made durable; never splice a dying session.

pub mod attach;
pub mod budget;
pub mod config;
pub mod repl;
pub mod snapshot;
pub mod stream;

/// Quote a SQL identifier (double-quote, escape embedded quotes).
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Quote a SQL literal (single-quote, escape embedded quotes).
pub fn quote_literal(lit: &str) -> String {
    format!("'{}'", lit.replace('\'', "''"))
}
