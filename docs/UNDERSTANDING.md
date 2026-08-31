# Understanding confirmation — ack invariant + SP1–SP8 (one page)

Written before first code, per the founder's kickoff prompt. Source: CLAUDE.md, wedge spec v0.4 §Amendment A, PNA-1.0.

## The ack invariant (Amendment A §A3, hardened per R3)

`confirmed_flush_lsn` on the source slot may advance to LSN `L` **only** when ALL of:

| # | Condition | Meaning in code |
|---|---|---|
| 1 | Durable prefix | Every frame from the previous ack through `L` is persisted in the log store (object_store; local fs now), in order, before the Standby Status Update is sent. Ack is gated on `graydb-log`'s `DurableUpTo`, never on receipt. |
| 2 | Transaction-complete boundary | `L` is a commit / stream-commit boundary. Never ack mid-transaction. |
| 3 | Checksummed + sequenced | Frames carry `{seq, lsn_start, lsn_end, txn_complete, crc32c}`; a prefix that fails verification is not durable. |
| 4 | Self-describing prefix | The durable stream from any ack point must decode without out-of-band state. Guaranteed structurally by rule 5. |
| 5 | Never splice a dying session | On any restart/raw-capture, NEVER continue a dying replication session's tail. Open a **fresh** replication session from the last durable ack; Postgres re-emits Relation/Type metadata for the new stream. |

Consequences I will enforce: slot advancement is decoupled from decode/apply health (rung 4 of the WAL ladder); replay = deterministic decode of the durable frame stream alone; crash between frame-durable and materialize is always safe (replay, zero dup/loss — that is Demo 4). The frame log is the write path of the database; everything downstream is disposable (I2/I3).

## SP1–SP8 map (each DONE = demo passes + graydb-check agrees)

| SP | Builds | Demo it must pass |
|---|---|---|
| SP1 | Attach pack (publication + event-trigger ddl_log, idempotent SQL), slot with exported snapshot, parallel ctid-range COPY of one schema, LSN0 recorded. graydb-check harness starts HERE. | D1: initial load ≡ SourceSnapshot(LSN0), row-multiset equality |
| SP2 | Durable frame log; ack only after durable write (invariant above) | D2: concurrent ingestion during load · D8: WAL-budget rungs 1–3 under induced stall |
| SP3 | Frame replay → typed changes; in-stream ddl_log consumption; LSN-versioned registry | D6: ADD COLUMN + DROP COLUMN flow through with per-LSN interpretation |
| SP4 | Columnar: parquet segments + roaring delete bitmaps + LSN-range footers; update = mark + reinsert | D3: update+delete via replica identity land correctly |
| SP5 | Tantivy search, commit-LSN batches, never mid-txn | (feeds D in SP8) |
| SP6 | Target-LSN reader over both shapes + graydb.stat_replication view (D-014 naming); SP6b = pgrx FDW proof (do not skip, do not block demo on it) | D5: caller-supplied target LSN returns the exact historical answer |
| SP7 | Chaos: crash-before-materialize replay; decoder kill → fresh-session restart from last durable ack; source-failover sim | D4 + D7: zero loss, zero duplicates, deterministic replay |
| SP8 | GrayDB Studio (axum + one static page) + scripted 8-minute runbook in docs/DEMO.md | The moment, end to end |

Guardrails held throughout: I1 no user writes; I4 every query path carries a target LSN (classes: eventual / bounded(X) / read_your_writes(token) / strong = source-barrier); I5 SQL-objects-only footprint + WAL budget min(50GB, 4h); non-goals (quorum log, undo, sharding, branching, write endpoints, control plane, k8s) are not stubbed. Any correctness path with a mock is a defect.
