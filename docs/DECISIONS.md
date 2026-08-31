# DECISIONS.md — spec-silent choices, recorded and moved past

Format: D-NNN · date · decision · why · revisit-if.

## D-001 · 2026-08-16 · Replication client: hand-rolled thin protocol client + tokio-postgres for SQL sessions

The CLAUDE.md question ("tokio-postgres replication mode vs hand-rolled pgoutput frame reader") is decided as a **hybrid**:

| Concern | Choice | Why |
|---|---|---|
| Replication session (CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT, IDENTIFY_SYSTEM, START_REPLICATION, CopyBoth, keepalive/ack) | Hand-rolled minimal client (`graydb-ingest::repl`) over `tokio::net::TcpStream`, using the `postgres-protocol` crate for SCRAM-SHA-256 only | (a) Frame custody: we persist raw pgoutput bytes and control Standby Status Update timing — the ack invariant lives at this boundary and must not be mediated by a library's buffering. (b) Mainline `tokio-postgres` does not expose the `replication=database` startup parameter; projects that use it for CDC run forks. A fork is a supply-chain and upgrade liability; the protocol surface we need is small (startup, auth, simple query, CopyBoth). |
| Everything SQL (attach pack, snapshot COPY workers, eligibility scan, graydb-check reads) | `tokio-postgres` 0.7 | Boring, correct, async, supports `copy_out` streaming and `SET TRANSACTION SNAPSHOT`. |

Revisit-if: mainline tokio-postgres ships replication mode with raw-frame access AND ack control; even then, frame custody argues for keeping ours.

## D-002 · 2026-08-16 · No git in this repo (founder instruction)

This is a completely local project; git is blocked. CLAUDE.md's "small PRs per SP" is honored as milestone discipline in `docs/MILESTONES.md` (self-rating + named untested surface per SP), not as git mechanics.

## D-003 · 2026-08-16 · SP1 staging format: raw COPY text parts + manifest.json

The parallel snapshot lands as PostgreSQL COPY text output, one directory per table, N part files (one per ctid range), plus `manifest.json` carrying LSN0, snapshot name, column lists, row/byte counts. Why: COPY text is the source's own canonical rendering — it IS the Type Interpretation Contract v0 (types render exactly as the source renders them); SP4 turns staged parts into parquet segments; parts are per-range idempotent restart units (wedge spec §5). Revisit-if: SP4 wants COPY BINARY for fidelity/speed — allowed, contract moves to binary decode rules then.

## D-004 · 2026-08-16 · SP1 check method: same-exported-snapshot multiset compare

`graydb-check` opens its own sessions, runs `SET TRANSACTION SNAPSHOT '<exported>'` (valid while the slot-creating replication connection stays open — the demo orchestrates that window), COPYs each full table, and compares sorted-line SHA-256 + row counts against the staged parts. Exact multiset equality, zero mocks, both sides at LSN0 by construction. Honest limitation: this check is only runnable inside the demo window; re-checking later requires a fresh snapshot export (SP2's frame log gives the durable comparison point thereafter). Memory note: sort happens in-process — fine at demo scale, named untested at multi-GB tables.

## D-005 · 2026-08-16 · Dev environment on this machine (Windows 11, no admin tooling present)

Canonical dev env stays docker-compose (PG 16 + 17, wal_level=logical) per CLAUDE.md. This machine had NO Rust, NO Docker, NO WSL, no psql. Founder approved (2026-08-16) user-scope installs: rustup (no admin/MSVC needed) + portable PostgreSQL 17.6 and 16.10 zip binaries (enterprisedb.com) run as local processes via `scripts/local-pg.ps1` — same ports/credentials/settings contract as docker-compose.yml (pg16→5416, pg17→5417, appdb/postgres/graydb, wal_level=logical, scram-sha-256 auth so the hand-rolled SCRAM path gets exercised). Tools live in `..\tools\` outside the repo tree.

Toolchain postmortem (found the hard way, do not re-derive): the plain `x86_64-pc-windows-gnu` target is broken on this box — rustup's self-contained MinGW ships no assembler, so raw-dylib import libs need llvm-mingw's `dlltool`, and GNU `ld` then mislinks those short import libraries: any binary touching them via mio/windows-sys (i.e. anything with tokio) dies at startup with STATUS_ACCESS_VIOLATION, while binaries that don't call through the stubs run fine. Resolution: build for **`x86_64-pc-windows-gnullvm`** with llvm-mingw 20260616 (ucrt + lld + llvm-dlltool, one consistent stack) and `+crt-static` so binaries carry no libunwind.dll dependency. Wired in `~/.cargo/config.toml` ([build] target + linker + rustflags); `cargo build/test/run` need no extra flags. Addendum (SP4): host-side builds (proc-macros/build scripts — the arrow tree pulls `const-random`→`getrandom` at compile time) still compiled for the gnu HOST triple and hit the same dlltool wall, so the default TOOLCHAIN is now `stable-x86_64-pc-windows-gnullvm` as well — host and target on one stack.

## D-006 · 2026-08-16 · Names and ports

Publication `graydb_pub`, slot `graydb_slot` (both in `graydb.toml`). Compose ports: PG16 → 5416, PG17 → 5417 (source defaults to 5417; override with `GRAYDB_SOURCE_PORT` to test both, per the "test both" working rule).

## D-007 · 2026-08-16 · Demo seed schema

Schema `app`: `customers` (PK, text, numeric, jsonb, timestamptz — exercises the v1 type list), `orders` (PK, FK, concurrent-write target), `notes` (no PK, `REPLICA IDENTITY NOTHING` → exercises append-only eligibility surfacing per Amendment A §A5.1). Seeded by `db/seed.sql`, dropped and recreated each demo run.

## D-009 · 2026-08-17 · Frame log storage: direct file IO + fsync now; object_store at S3 time

CLAUDE.md's stack line says `object_store`; the ack invariant says DURABLY-STORED. object_store's local backend does not promise fsync, and the invariant outranks the stack line (spec-over-convenience rule). So: segments are written with direct file IO + `sync_all` on every commit frame, laid out one-segment-per-object so the object_store backend drops in when S3/MinIO lands. Sync-per-commit is the default; batched group-sync is a later performance knob and never a default weakening.

## D-010 · 2026-08-17 · Frame granularity + ordering contract

One frame per XLogData message (payload = raw pgoutput bytes, untouched). txn_complete = payload is a Commit ('C'); the ack boundary is the commit's end_lsn (already one-past-transaction). Verification asserts seq contiguity for ALL frames but LSN monotonicity only across COMMIT frames — pgoutput delivers transactions in commit order while interior record LSNs interleave across concurrent transactions. (First demo run failed on exactly this; the checker was wrong, not the stream.)

## D-011 · 2026-08-17 · Ladder rung-2 trigger + demo budget

Spec gives rung 1 at ≥50% of budget but is silent on rung 2's threshold: chosen 70% (`shed_fraction` in graydb.toml). Rung 3 is condition-triggered (write path degraded), not percentage-triggered. Demos use a 4 MiB budget (env `GRAYDB_WAL_BUDGET_BYTES`) so the ladder walks in seconds on a call; the production default stays min(50 GiB, 4 h).

## D-012 · 2026-08-17 · Registry version boundaries = first in-stream use

A schema version's `valid_from_lsn` is the commit end_lsn of the transaction carrying the NEW Relation message — the schema's first in-stream use — not the ALTER's own commit. Between the ALTER and first use, the stream has no shape claim to make; the ALTER's exact position is carried by the in-stream ddl_log event (both are asserted in Demo 6). Registry keys on relation OID so renames don't fork identity (matrix #4/#11). sql_drop fires once per dropped object (a dropped column AND its default = 2 rows + 1 ddl_command_end row) — capture keeps all rows; consumers filter by `kind`.

## D-014 · 2026-08-17 · User-facing naming follows PostgreSQL ecosystem conventions (founder ruling)

Founder direction: every user-facing name (SQL objects, views, columns, Studio labels) must read like the database ecosystem, not data-SaaS/AI vocabulary. Applied:

| Old (spec draft) | New (shipped) | Modeled on |
|---|---|---|
| `graydb.freshness` view | **`graydb.stat_replication`** | `pg_stat_replication` / `pg_stat_subscription` |
| "freshness view" (prose) | "replication status view" | — |
| planned columns | `shape, received_lsn, applied_lsn, apply_lag_bytes, apply_lag_ms` | pg_stat_replication's `*_lsn` / `*_lag` columns |

Kept (already standard vocabulary): `SET graydb.target_lsn` (GUC-style), consistency classes `eventual | bounded(X) | read_your_writes(token) | strong`, `restart_lsn`/`confirmed_flush_lsn` terminology, "replication slot", "publication", "replica identity", "WAL retention". The locked spec documents (wedge v0.4, PNA-1.0) are historical records and keep their original wording; CLAUDE.md and all forward-looking docs/code use the new names. SP6 implements the view under the new name from day one.

## D-016 · 2026-08-17 · `strong` reads terminate on the STREAM position, not the commit position

Amendment A A4 says strong = source barrier: take B = `pg_current_wal_lsn()`, wait for shapes ≥ B. Implemented naively this never terminates: shape watermarks only advance at commit LSNs, while B routinely sits past the last commit (checkpoints, standby-snapshot records, and other non-replicated WAL move the head). Decision: the pump tracks `stream_lsn` — the highest source WAL position the session has SEEN (keepalive `wal_end` or frame end) — and strong waits for `stream_lsn >= B AND applied >= durable`. That is the correct proof: once the stream has passed B, every transaction that committed at or before B has been received; once apply reaches the durable mark, they are materialized. The 1s status tick now sets `request_reply=true` so an idle source still answers with its `wal_end`. Found by clicking "strong" in Studio; measured 665 ms end-to-end on a live source.

## D-017 · 2026-08-17 · GrayDB never writes to the source to make its own gauge look better

PostgreSQL releases retained WAL only when `restart_lsn` advances, which requires the decoder to consume a standby-snapshot record FOLLOWED by a commit. On a fully idle source, a recovered-and-caught-up GrayDB therefore still shows high retained WAL. Two options were prototyped: (a) have Studio issue `pg_log_standby_snapshot()` + a small write to force the release, or (b) report it honestly. Chose (b): (a) means writing into the customer's database, which violates I1/I5 and the whole "SQL objects only" promise for a cosmetic dashboard win. The UI now states the mechanism inline, and DEMO.md Beat 7 keeps a write trickle running so recovery is visible on camera (verified: gauge 113.5% → 0.0% once commits flow).

## D-015 · 2026-08-17 · RetryDirectory for tantivy on AV-taxed Windows hosts

Evidence chain: tantivy commits intermittently fail with ERROR_ACCESS_DENIED creating brand-new uniquely-named segment files (2nd+ commit in a burst), on this box only; a pure std::fs churn probe (2,400 create_new files, GC-style deletes, same directory tree) never fails; the same tantivy flow passes in isolation and fails under bursts; failures hit random components (.del/.fast/.store/.fieldnorm/.term) and vanish on retry. Diagnosis: endpoint-security minifilter racing file creates (no admin rights to inspect `fltmc` or set exclusions). Fix: `graydb-search::retry_dir::RetryDirectory` wraps MmapDirectory and retries PermissionDenied on open_write/atomic_write/delete (40 × 25 ms, then loud failure). Full mmap + on-disk persistence retained; passthrough cost on healthy hosts is zero. Revisit-if: prod targets are Linux (this wrapper simply never triggers there).

Also fixed en route: search unit tests moved out of `%TEMP%` into `target/test-tmp` (same AV, worse odds in %TEMP%).

## D-018 · 2026-08-17 · Research Target #1 adopted: GrayDB vs ClickHouse under continuous CDC

Founder-relayed architect feedback ratified as the next phase. Full protocol, hypothesis, kill criterion and scale plan live in docs/RESEARCH-R1.md. Key rulings: (a) assume ClickHouse wins static OLAP until proven otherwise; (b) the hypothesis is stability under churn at equal correctness, not scan speed; (c) no architecture changes until benchmarks prove or kill the hypothesis; (d) ClickHouse has no native Windows build, so the head-to-head runs on the Linux stage (the same box SP6b needs) — local runs only debug GrayDB itself. Prerequisites P1–P4 (incremental log tail, overlay-over-segments freshness, streaming TableProvider scan, O(1) row counters) are implementations of already-designed shapes — `graydb-log`'s charter always said `tail(from_lsn)`, and T5's measured shape was always "visibility predicate inside the scan" — not architecture changes.

## D-013 · 2026-08-17 · Columnar storage decisions (SP4)

- Per-type mapping table v0 (Amendment A A5.2): int2/int4/int8/oid → Int64, float4/float8 → Float64, bool → Boolean, EVERYTHING else (incl. numeric, timestamps, jsonb, uuid) → Utf8 carrying the source's exact text rendering. Numeric stays text: exactness over scan speed in v1; typed decimal lands when the reader needs it and can prove round-trip fidelity.
- Every segment carries a `__gdb_lsn` UInt64 column (row insert LSN) + footer metadata `graydb.lsn_min/lsn_max/table` — the visibility predicate `insert_lsn <= L && !deleted_by(<= L)` needs per-row insert LSNs, not just the segment range.
- Delete sidecar file = JSON `[(row_idx, delete_lsn)]` per segment; the roaring bitmap is the in-memory mask built from it. Rationale: time travel needs the delete LSN, a bare bitmap loses it. Binary sidecar format is a later optimization.
- Flush threshold `columnar.flush_rows` = 100k (spec silent; chosen). Segments roll only on flush; compaction is post-spike.
- PK→location index is in-memory, rebuilt by scan on open (I3: everything reconstructible; persistent index is an optimization decision for later).
- Update for an unknown key / change-shape drift vs store schema = LOUD failure, never skip (mock-free rule).
- arrow/parquet pinned to 58 because datafusion 54 (SP6 reader) requires ^58.3 — aligned now to avoid a two-arrow workspace later.

## D-008 · 2026-08-16 · Event-trigger capture excludes schema `graydb`

The ddl_log capture functions skip objects in schema `graydb` to avoid self-noise (attach pack re-runs, internal DDL). The reconciler layer (post-spike) remains the safety net for anything filtered. Revisit-if: a real DDL on graydb.* ever needs to flow to shapes — it doesn't in Act 1 (that schema is ours, not customer data).
