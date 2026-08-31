# CLAUDE.md — GrayDB build instructions for Claude Code

You are building **GrayDB**: a derived database for PostgreSQL. It attaches to an existing
Postgres over logical replication, lands every change in its own durable LSN-ordered log,
materializes two read shapes (columnar analytics + full-text/vector search), and serves them
through a Postgres-compatible surface where every query can declare a consistency class and
prove the exact source LSN it reflects. Never describe it as a "sync tool" or "read-only layer" —
it is a database whose write API is the replication protocol.

**Read before writing any code:** `docs/graydb_product_architecture_north_star_PNA1.md` (the locked
architecture), `docs/graydb_wedge_spec_v0.4.md` (Act-1 spec incl. Amendment A — the governing document
for THIS repo), `docs/memory.md` (project state). The other docs are context and evidence.

## What THIS repo builds: S1-lite demo + GrayDB Studio
Scope = the demo with a moment, not the full product. On a live call we must be able to:
attach to a Postgres, backfill one schema, stream changes, run a schema change, kill the source
connection and the decoder mid-stream, restart — and show graydb.stat_replication converge to the
exact LSN with zero loss and zero duplicates, then query columnar + search through SQL at a
caller-supplied target LSN.

## Constitution — these override any convenient shortcut
- I1: the source Postgres is the only writer. GrayDB accepts no user writes anywhere.
- I2: one durable LSN-ordered log is the spine; every materialized byte must be derivable from it.
- I3: shapes are disposable. Any store must be rebuildable by snapshot + log replay. No state may
  exist that cannot be reconstructed.
- I4: every query path carries a target LSN. Classes: eventual | bounded(X) | read_your_writes(token)
  | strong (strong = source-barrier: fetch pg_current_wal_lsn() from source, wait shapes >= it).
- I5: the source is sacred. SQL-objects-only footprint (publication + event triggers), hard WAL
  retention budget (default min(50GB, 4h)) with the shed ladder, capped backfill read impact.

## Non-negotiable correctness rules (from Amendment A — do not weaken)
- Slot acknowledgment may advance only past a transaction-complete, checksummed, DURABLY-STORED
  frame prefix that is self-describing. Raw-capture/restart NEVER splices a dying replication
  session: always open a fresh session from the last durable ack so Postgres re-emits Relation/Type
  metadata.
- Invariant (the product): Materialized(table, L) is semantically equivalent to SourceSnapshot(table, L)
  under the type-interpretation and table-eligibility contracts in the wedge spec §Amendment A.
- Eligibility: PK or replica identity required for update/delete tables; REPLICA IDENTITY NOTHING
  = append-only eligibility; unchanged TOAST columns reconstructed from the prior materialized version.
- graydb-check (the invariant harness) is built ALONGSIDE from milestone SP1, not after. Any
  correctness path with a mock is a defect.

## Explicit NON-goals for this repo (do not build, do not stub "for later")
No quorum log. No undo engine. No sharding/owner routing. No branching. No write endpoints.
No multi-tenant control plane. No Kubernetes. Single node, single source, config file, one binary
per concern.

## Stack (locked for the spike)
- Rust stable, tokio. Logging: tracing. Errors: thiserror/anyhow at edges.
- Replication client: evaluate `tokio-postgres` replication mode vs a thin hand-rolled pgoutput
  frame reader over the replication protocol. Criteria: access to raw frames (we persist frames,
  not interpretations), streaming txn support, keepalive/ack control. Hand-rolled is acceptable —
  we need frame-level custody for the ack invariant.
- Log frames: our own format — sequence, source LSN range, txn boundary flag, crc32c, then payload =
  raw pgoutput bytes. Storage via `object_store` crate (local filesystem now; S3/MinIO later free).
- Columnar: arrow-rs + parquet (zstd, dictionary on) + a delete-bitmap sidecar (roaring) + per-segment
  LSN range in footer metadata. Query for demo: datafusion.
- Search: tantivy; index declared per table+columns in config; commit in LSN batches, never mid-txn.
- Reader surface: milestone-gated. SP6a (demo-grade): `graydb-studio` server exposes SQL over
  datafusion + a tantivy `search()` table function, plus `graydb.stat_replication` as a virtual
  table (pg_stat_replication-style: shape, received_lsn, applied_lsn, apply_lag; D-014 naming).
  SP6b (the real S1 question): a `pgrx` extension in `extension/` embedding the same reader — start
  with an FDW exposing tables with LSN-visibility pushdown; custom scan provider is the stretch.
  SP6b is the extension-surface proof; do not skip it, but do not block the demo on it.
- Studio (GrayDB Studio, pgAdmin-flavored, deliberately minimal): axum + one static HTML/JS page
  (no build chain). Panels: Attach (conn string, publication/slot status), Tables (eligibility,
  per-shape applied LSN, lag), SQL editor + results grid with consistency-class dropdown and an
  "LSN proof" footer on every result, WAL-budget gauge, Chaos buttons (kill decoder / drop network /
  crash before materialize) that exercise SP7 live, and an event log. Dark, dense, boring, honest.

## Milestones — each is DONE only when its demo passes and graydb-check agrees
- SP1 Attach + snapshot: create publication + event-trigger ddl_log pack (plain SQL, idempotent),
  create slot with exported snapshot, parallel COPY one schema at the snapshot, record LSN0.
  Demo 1: exported-snapshot initial load. Check: row-multiset equality at LSN0.
- SP2 Frame log: consume from LSN0, persist frames per the ack invariant, ack the slot only after
  durable write. Demo 2: concurrent ingestion during load. Demo 8: WAL budget rungs 1–3 under an
  induced stall (pause the log writer; watch budget gauge; resume).
- SP3 Decode + registry: replay frames → typed changes; consume ddl_log rows in-stream; registry =
  LSN-versioned schema table. Demo 6: one additive DDL (ADD COLUMN) + one destructive (DROP COLUMN)
  flow through with correct per-LSN interpretation.
- SP4 Columnar: segment writer with LSN ranges + delete bitmaps; update = bitmap-mark + reinsert.
  Demo 3: update+delete via replica identity land correctly.
- SP5 Search: tantivy apply in commit order. (Feeds demo in SP8.)
- SP6 Reader: target-LSN query (`SET graydb.target_lsn` equivalent parameter) over both shapes;
  graydb.stat_replication view. Demo 5: caller-supplied target-LSN query returns the exact
  historical answer. SP6b: pgrx FDW proof.
- SP7 Chaos: crash after frame-durable but before materialize → replay, zero dup/loss (Demo 4);
  kill decoder mid-stream → fresh-session restart from last durable ack, deterministic replay
  (Demo 7); source failover simulation = restart source container.
- SP8 Studio + demo script: wire everything; scripted 8-minute "moment" runbook in docs/DEMO.md.

## Working rules
- Small PRs per SP; each ends with `cargo test` green and the SP's demo runnable via
  `just demo-spN` (add a justfile).
- Local dev: docker-compose with Postgres 16 AND 17 (wal_level=logical) — test both.
- Every magic number from the spec goes in `graydb.toml` with the spec default, never inline.
- When the spec and convenience conflict, the spec wins; when the spec is silent, decide, note it
  in docs/DECISIONS.md, move on. Ask the founder only for: pricing, naming, scope additions.
- Self-rate honestly in PR descriptions; name what is untested.
