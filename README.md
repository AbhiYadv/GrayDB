# GrayDB

A **derived database for PostgreSQL**. It attaches to an existing Postgres over logical
replication, lands every change in its own durable LSN-ordered log, materializes two read
shapes (columnar analytics + full-text search), and serves them through SQL where **every
query can declare a consistency class and prove the exact source LSN it reflects**.

It is not a sync tool and not a read-only cache: its write API *is* the replication protocol.
Your PostgreSQL remains the only writer — GrayDB accepts no user writes anywhere (invariant I1).

This repo is the **S1-lite spike**: the eight-milestone demo that proves the correctness
claims, plus GrayDB Studio (a pgAdmin-flavoured GUI). 100% Rust.

---

## Quickstart

**macOS / Linux** (the easy path):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh    # Rust
brew install postgresql@17        # or: apt install postgresql-17
#   set wal_level = logical, max_replication_slots = 8, max_wal_senders = 8; restart

cargo test --workspace            # ~22 unit tests
cargo run --release -p graydb-studio        # http://127.0.0.1:7432 → click Attach
```

**Windows** needs a specific toolchain (the plain `windows-gnu` target is broken for this
dependency tree) — see [docs/SETUP.md](docs/SETUP.md), which also covers portable PostgreSQL,
attaching to your own database, and the managed-Postgres caveat.

Point it at your own source by editing the `[source]` block in `graydb.toml`
(host / port / dbname / user / schema) and `[[search.indexes]]` for searchable columns.
Real credentials belong in `GRAYDB_SOURCE_PASSWORD`, never in the committed file.

## What it does, concretely

```
PostgreSQL ──logical replication──▶ durable frame log (crc32c, fsync-gated ack)
                                          │
                          ┌───────────────┴────────────────┐
                          ▼                                ▼
              columnar segments                     tantivy indexes
        (parquet+zstd, delete bitmaps,           (BM25, commit-LSN batches)
         per-row LSN visibility)                          │
                          └───────────────┬────────────────┘
                                          ▼
                        DataFusion SQL @ any target LSN + LSN proof
```

Query the past as easily as the present:

```sql
-- through Studio's SQL editor, consistency class = target_lsn=0/1A2B3C4D
SELECT status, count(*) FROM app.orders GROUP BY status;
-- → answers exactly as of that source LSN, with the proof attached to the result

SELECT c.name FROM search('app.customers', 'quokka') s
JOIN app.customers c ON CAST(c.id AS VARCHAR) = s.key;
```

## Status

All eight milestones built; demos pass on **PostgreSQL 16.10 and 17.6**.

| Milestone | Proves | State |
|---|---|---|
| SP1 attach + snapshot | initial load == source snapshot at exactly LSN0 | done |
| SP2 frame log | slot ack never outruns fsync; WAL-budget ladder rungs 1–3 | done |
| SP3 decode + registry | DDL in-stream, correct per-LSN interpretation | done |
| SP4 columnar | update/delete via replica identity; time travel | done |
| SP5 search | commit-LSN batches, idempotent replay | done |
| SP6a reader | target-LSN SQL over both shapes; `graydb.stat_replication` | done |
| SP6b pgrx extension | `psql` access via FDW | **open** (needs Linux/macOS) |
| SP7 chaos | decoder kill / crash-before-materialize / source failover → zero loss, zero dup | done |
| SP8 Studio | the GUI + 8-minute runbook ([docs/DEMO.md](docs/DEMO.md)) | done |
| R1 benchmark | vs ClickHouse under continuous CDC ([docs/RESEARCH-R1.md](docs/RESEARCH-R1.md)) | harness built; ClickHouse column needs Linux/macOS |

## Honest limitations

- **No writes.** By design (I1), not by omission.
- **Correctness matrix incomplete**: 2 of the 20 DDL patterns are exercised; the
  property-based harness (gate W3) is post-pilot.
- **Not queryable from `psql` yet** — that's SP6b.
- **Performance unmeasured at scale.** Quoted numbers are laptop-scale; the real
  GrayDB-vs-ClickHouse table needs the Linux stage.
- **Managed Postgres untested** — `CREATE EVENT TRIGGER` needs superuser; RDS/Aurora/Cloud SQL
  behaviour unverified (a degraded catalog-diff mode is specified but unbuilt).
- **Studio is a demo tool**: localhost-only, no auth, no service supervision.
- No vectors/HNSW, no compaction — deliberately out of this repo's scope.

## Layout

```
crates/graydb-ingest      attach pack, replication client, snapshot COPY, WAL budget
crates/graydb-log         the durable frame log (the spine) + incremental tail
crates/graydb-registry    pgoutput decode + LSN-versioned schema registry
crates/graydb-columnar    parquet segments + delete bitmaps + LSN visibility
crates/graydb-search      tantivy indexes, commit-LSN batched
crates/graydb-studio      reader (DataFusion) + axum server + GUI
crates/graydb-check       the invariant harness: every demo + the R1 benchmark
extension/                SP6b pgrx extension (open)
docs/                     architecture, spec, decisions, milestones, setup, demo runbook
```

Start with [CLAUDE.md](CLAUDE.md) (build constitution), then
[docs/MILESTONES.md](docs/MILESTONES.md) (what's proven, what isn't) and
[docs/DECISIONS.md](docs/DECISIONS.md) (every spec-silent call and why).
