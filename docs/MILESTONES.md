# MILESTONES.md — per-SP status + honest self-rating (replaces PR descriptions; D-002 no-git)

## R1 — GrayDB vs ClickHouse under continuous CDC · status: harness built, GrayDB column measured locally; ClickHouse column awaits the Linux stage

Adopted from architect feedback 2026-08-17 (D-018; protocol in docs/RESEARCH-R1.md). Work done:

| Piece | What changed |
|---|---|
| P1 `graydb-log::tail::LogTail` | Incremental frame cursor (charter's `tail(from_lsn)`) with truncation/rewind awareness; kills the O(N²) full-replay-per-tick |
| P1 `graydb-registry::decoder::StreamDecoder` | Stateful incremental decode; `abort_open_txn` pairs with log rewind; seq-gap = loud failure |
| P2 overlay-over-segments | Engine no longer finalizes per tick; open rows served from a typed in-memory overlay; segments flush at `columnar.flush_rows` only (no tiny-segment spray under churn) |
| P3 `graydb-studio::provider` | `LsnTableProvider`/`LsnScanExec`: streaming parquet scan with segment pruning, projection pushdown, per-batch LSN+bitmap masking — T5's shape; replaces MemTable-copy-per-query for BOTH the live engine and the disk Reader (demo-sp6 re-verified PASS) |
| P4 | O(1) visible-row counters (status no longer full-scans) |
| Harness | `bench-cdc` (`just bench-r1`): quiet vs heavy phases, Q1/Q2 latency percentiles, source→visible freshness sampler, and pause-drain-compare exactness probes (count + sum at a captured LSN must match the source exactly, else the run is void) |

Named untested/limits: local scale only (1M rows / 300 tps on a laptop — the 1B-row head-to-head needs Linux); CPU/bytes-read columns not instrumented yet; sync parquet IO inside the scan stream (spawn_blocking bridge later); no compaction so long CDC runs accumulate segments; Q1/Q2 carry CAST overhead from D-013 numeric/timestamp-as-text.

## SP1 — Attach + snapshot · status: DONE (founder ran and watched Demo 1 pass on both majors, 2026-08-17 IST: PG16 LSN0 0/339C4C0, PG17 LSN0 0/31BC1A8)

Demo 1 results (2026-08-16, `cargo run -p graydb-check --bin demo-sp1`):

| | PG 17.6 (:5417) | PG 16.10 (:5416) |
|---|---|---|
| LSN0 | 0/1FC3768 | 0/1FD4AE8 |
| Rows staged at LSN0 | 28,000 (3 tables, 24 parts, 8 streams) | 28,000 |
| Concurrent writes during COPY | 13,500 — all excluded | 17,800 — all excluded |
| Multiset equality at LSN0 | PASS ×3 tables | PASS ×3 tables |
| Eligibility surfaced | full / full / append-only (RI NOTHING) | same |
| ddl_log live capture | CREATE PUBLICATION + ALTER TABLE | same |
| `cargo test --workspace` | green | (same binaries) |

Founder gate: run `just demo-sp1` and `just demo-sp1-pg16` yourself (start sources first: `.\scripts\local-pg.ps1 -Action start`) — SP1 is DONE only after you watch it pass.

### Original pre-verification record (kept for honesty)

Delivered:
- Attach pack (`crates/graydb-ingest/sql/attach_pack.sql`): idempotent, SQL objects only — schema `graydb`, `ddl_log`, event triggers on `ddl_command_end` / `sql_drop` / `table_rewrite`.
- Publication create (schema + `graydb.ddl_log` in-stream), eligibility scan surfacing Amendment A A5.1 classes (full / RI-FULL-warning / append-only).
- Minimal replication-protocol client (`repl.rs`, D-001): startup with `replication=database`, trust/cleartext/md5/SCRAM-SHA-256 auth, simple query, `IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT ... (SNAPSHOT 'export')` → LSN0 + snapshot name.
- Parallel ctid-range COPY at the exported snapshot (`snapshot.rs`), open-ended final ranges (stale relpages can never lose rows), COPY-text staging + `manifest.json` (D-003).
- graydb-check multiset checker (D-004) with unit tests, and the `demo-sp1` driver: seed → attach → slot/LSN0 → COPY under concurrent writes → multiset check at LSN0 → live DDL into ddl_log → verdict.

Self-rating after reality contact: **8 / structure and correctness both exercised end-to-end on two PG majors.** Cap honored (calibration rule: ≤8.5 until external review).

Items 1–5 of the original untested list are now tested by the passing demo (compilation, hand-rolled replication protocol incl. SCRAM-SHA-256 against real 16.10/17.6, N concurrent snapshot-importing sessions, TID-range COPY on both majors). Still genuinely untested:
1. COPY-text line splitting + in-memory sort at multi-GB scale (D-004 named limitation).
2. Badly stale relpages (open-ended final range covers it by construction, but never provoked deliberately).
3. Event-trigger pack under permission-restricted roles (demo uses superuser; managed-PG posture is a WL2 concern for later SPs).
4. Behavior when the replication connection drops mid-load (snapshot vanishes; per-table restartability is designed but not chaos-tested — SP7 territory).

## SP2 — Frame log + ack invariant · status: DONE (founder ran and watched both demos pass, 2026-08-17 IST; post-gate demo-choreography fix: settle loop now forces standby snapshots so the gauge starts <1% and walks the rungs visibly — verified same day)

Demo 2 + Demo 8 results (2026-08-17, `just demo-sp2` / `demo-sp2.cmd`):

| | PG 17.6 | PG 16.10 |
|---|---|---|
| Frames landed (crc32c, seq-contiguous) | 46,018 / 808 commits | 61,612 / 1,105 commits |
| Durable ack == slot confirmed_flush | PASS (0/64C0138) | PASS (0/49D07F0) |
| Commit-order LSN monotonicity | PASS | PASS |
| SP1 multiset at LSN0 (still enforced) | PASS ×3 | PASS ×3 |
| Ladder rung 1 warn ≥50% | PASS (61.7%) | PASS (54.9%) |
| Ladder rung 2 shed ≥70% | PASS (75.5%) | PASS (68.6→78.9%) |
| Ladder rung 3 spill to staging | PASS (5,252 frames) | PASS (5,454 frames) |
| Recovery below warn after resume | PASS (0%) | PASS (0%) |

What SP2 enforces in code: Standby Status Update reports ONLY graydb-log's durable mark — a transaction-complete, checksummed, fsync'd prefix (sync on every commit frame; batching is a later knob, never a default). Stall (rung 3) freezes the mark and diverts frames to staging; resume drains staging in order, syncs, then acks.

Self-rating: **8** — both demos exercised end-to-end on two majors, first-run failure was in the checker (interior-LSN interleave misunderstanding), found and fixed by the demo itself, which is the harness doing its job.

Named untested surface:
1. Crash-restart resume of an EXISTING log (fresh-session-from-last-durable-ack) — that is Demo 7 / SP7, deliberately not faked here.
2. Segment roll at 64 MiB (demo volume stays under one segment).
3. Oversized/streamed transactions (proto v1, no streaming; Amendment A A5.5 spill cap untested).
4. Staging spill is mirrored in memory (Vec) — bounded only by demo scale; real NVMe-backed spill lands with SP7 hardening.
5. Long stalls vs wal_sender_timeout: keepalive replies (restating frozen durable) continue during stall, but stalls > 60 s not yet exercised.

Founder gate: run `.\demo-sp2.cmd` and `.\demo-sp2-pg16.cmd` (or `just demo-sp2*`) and watch the ladder walk.

## SP3 — Decode + LSN-versioned registry · status: DONE (founder ran and watched Demo 6 pass on both majors, 2026-08-17 IST)

Demo 6 results (2026-08-17, `just demo-sp3` / `demo-sp3.cmd`): three insert eras around `ADD COLUMN city` then `DROP COLUMN city` on app.customers, everything replayed FROM THE DURABLE FRAME LOG ALONE:

| Check | PG 17.6 | PG 16.10 |
|---|---|---|
| Registry versions 6 → 7(+city) → 6 cols at correct commit-LSN boundaries | PASS | PASS |
| Every era insert decodes under its era's schema (city present only in era 2) | PASS | PASS |
| `schema_for_table("app.customers", L)` correct at sampled LSNs per era | PASS | PASS |
| Both ALTERs captured in-stream, commit-LSNs strictly between eras | PASS | PASS |
| Replay deterministic (two replays byte-identical) | PASS | PASS |

Delivered: pgoutput v1 parser (`graydb-registry::pgoutput` — Begin/Commit/Relation/Type/Insert/Update/Delete/Truncate/Origin/Message, text tuple values kept exactly as the source rendered them), LSN-versioned registry keyed by relation OID (rename-stable, matrix #4/#11), typed-change replay with per-txn commit-LSN assignment, in-stream ddl_log consumption, registry persistence to JSON (rebuildable from log — I3).

Self-rating: **8** — full pipeline exercised both majors; two demo-checker calibrations found by running (sql_drop fires per dropped object incl. column defaults; registry version boundary = schema's first in-stream USE, not the ALTER's own commit — the ddl_log event carries the ALTER position, both asserted).

Named untested surface:
1. Only matrix patterns #1 (ADD COLUMN w/ constant default) and #3 (DROP COLUMN) exercised — the other 18 classes are W3/harness scope, deliberately post-pilot per the sequencing ruling.
2. Update/Delete decode parsed but not demo-asserted (Demo 3 / SP4 does that via replica identity).
3. Table rewrites (class C), TOAST 'u' values, Truncate apply semantics — parser handles, apply does not exist yet.
4. Type ('Y') messages recorded but no per-type mapping table yet (Amendment A A5.2 lands with SP4 typed materialization).

Founder gate: run `.\demo-sp3.cmd` and `.\demo-sp3-pg16.cmd`.

## SP4 — Columnar materialization · status: DONE (founder ran Demo 3 on both majors — PG17 twice, PG16 once — all PASS, 2026-08-17 IST; observed dev-build apply throughput 289K–664K changes/s across runs)

Demo 3 results (2026-08-17, `just demo-sp4` / `demo-sp4.cmd`): backfill (20,000 orders + 5,000 customers + 3,000 notes) → flushed parquet segment 0 at LSN0; then 2,000 streamed inserts, 226 updates (hitting backfill AND streamed rows via replica identity), 166 deletes, 75 second-wave updates:

| Check | PG 17.6 | PG 16.10 |
|---|---|---|
| Head multiset == source current state (3 tables) | PASS (21,834 orders) | PASS |
| Row counts at LSN0 (20,000) and L1 (22,000) | PASS | PASS |
| Update via replica identity (id=97: paid → reprocessed) | PASS | PASS |
| Delete via replica identity (id=131: visible@L1, gone@head) | PASS | PASS |
| Update-of-update version chain (id=291: reprocessed → re-reprocessed) | PASS | PASS |
| Apply throughput (dev build, unoptimized) | 2,467 changes in 3.7 ms ≈ 664K changes/s | comparable |

Delivered: `graydb-columnar` — parquet segments (zstd level 3, dictionary on, `graydb.lsn_min/max` footer metadata, per-row `__gdb_lsn`), roaring delete-bitmap sidecars carrying delete LSNs (time travel needs them), update = bitmap-mark + reinsert, PK→location index, unchanged-TOAST reconstruction path (A5.3), COPY-text bridge for backfill, LOUD failure on unknown keys/shape drift. Type mapping v0 per D-013.

Self-rating: **8**. Named untested surface:
1. timestamptz rendering parity between walsender and COPY sessions — created_at deliberately excluded from the multiset compare until proven; the other 4 columns compare exactly.
2. TOAST reconstruction implemented but not demo-exercised (no >2KB values in seed); needs a dedicated case in the W3 harness.
3. Float64 render round-trip (no float columns in seed; numeric stays text by design).
4. Compaction/bitmap folding not built (post-spike by charter).
5. Apply throughput number is dev-build, in-memory-index, demo-scale — a real number comes from release builds at SP6 measurement time.

Founder gate: run `.\demo-sp4.cmd` and `.\demo-sp4-pg16.cmd`.

## SP5 — Search (tantivy, commit-LSN batches) · status: DONE (founder watched PG17 pass twice, 2026-08-17 IST; PG16 founder-initiated + passed 2× supervised)

Demo results (2026-08-17, `just demo-sp5` / `demo-sp5.cmd`): 8,000 backfill docs at LSN0 (customers: name+email, notes: body), then 153 txns streamed — 500 inserts, an update chain (unicorna → xylophone) on a backfill row, a delete, 300 notes:

| Check | PG 17.6 | PG 16.10 |
|---|---|---|
| Doc counts == source (5,499 + 3,300) | PASS | PASS |
| Inserts searchable (zephyr=499) | PASS | PASS |
| Update = delete+re-add (xylophone→[42], stale unicorna=0) | PASS | PASS |
| Delete drops the doc (zqmail1=0, zqmail2=1) | PASS | PASS |
| Commit-LSN batching, boundary-only (153 txns → 3 batch commits @64) | PASS | PASS |
| applied_lsn watermark coherent across indexes | PASS | PASS |
| Full stream re-apply idempotent (crash-replay property) | PASS | PASS |

Delivered: `graydb-search` — declared per table+columns in graydb.toml, BM25 via tantivy 0.26, document identity = replica-identity values (keyless tables get deterministic synthetic keys from the replay's global change index, so re-apply never duplicates), commits ONLY at txn boundaries in LSN batches, applied_lsn watermark persisted per index. Plus `RetryDirectory` (D-015): this dev box's endpoint security intermittently EACCES-es tantivy's commit-burst file creates; retries with backoff absorb it.

Self-rating: **7.5** — core semantics proven on both majors; the environmental EACCES hunt consumed real time and the fix is a workaround, not a root-cause kill (no admin rights to inspect/exclude the AV).

Named untested surface:
1. BM25 relevance quality (only exact-token assertions; fine for the demo's purpose).
2. Vectors/HNSW — wedge-spec scope, deliberately NOT in the S1-lite repo (CLAUDE.md locks tantivy FTS only).
3. Merge throttling and large-index behavior (demo indexes are tiny; merges left on default policy).
4. Crash mid-batch recovery is proven only at full-replay granularity; a partial-batch crash schedule belongs to SP7.
5. RetryDirectory on healthy hosts: harmless passthrough, but only exercised on this AV-taxed box.

Founder gate: PG17 watched twice, PASS (2026-08-17 IST); PG16 founder run initiated — pending its output paste (passed twice in supervised runs). Demo output noise fixed post-gate: tantivy INFO logs silenced by default.

## SP6 — Reader (SP6a) · status: DONE (founder ran and watched Demo 5 pass on BOTH majors, 2026-08-17 IST) · SP6b OPEN (pgrx needs Linux — founder environment decision pending)

Demo 5 results (2026-08-17, `just demo-sp6` / `demo-sp6.cmd`):

| Check | PG 17.6 | PG 16.10 |
|---|---|---|
| `count(*)` at LSN0 / L1 / head = 20,000 / 22,000 / source-now | PASS | PASS |
| Row-level history: `status(id=97)` = 'paid' @L1, 'reprocessed' @L2 | PASS | PASS |
| `search('app.customers','xylophone')` JOIN columnar → 1 row, right name | PASS | PASS |
| `graydb.stat_replication`: 4 shapes, received/applied/lag honest | PASS | PASS |

Delivered: `graydb-studio` reader library — DataFusion 54 session per query, every columnar table registered at the caller's target LSN (visibility applied from disk artifacts only: manifest + parquet + delete sidecars — a reader needs nothing but directories, I3), `search(table, query)` as a DataFusion table function joinable to SQL, `graydb.stat_replication` under its D-014 name, and an `LsnProof` on every result (target, received, per-shape applied). Also `graydb_columnar::reader::read_visible_batches` (typed batches, LSN-filtered) and read-only `SearchReader`.

Self-rating: **7.5** — the semantics Demo 5 demands are exact on both majors. Honest architecture note: tables materialize into MemTables per query (correct, demo-scale); segment-pushdown TableProvider is the performance path and belongs with the release-build measurement pass.

Named untested/undone surface:
1. **SP6b (pgrx FDW proof) NOT DONE** — pgrx does not build PostgreSQL extensions on Windows; it needs a Linux environment (or WSL, which this machine lacks). CLAUDE.md says do-not-skip, so it stays OPEN as SP6b-pending-environment — founder decision needed on where it runs.
2. Consistency classes at the reader (bounded/strong wait semantics) — Demo 5 only needs target-LSN; classes wire up with the live Studio server (SP8) where a live source heartbeat exists.
3. Per-query MemTable copy cost unmeasured; release-build numbers deferred to the measurement pass.
4. numeric stays Utf8 in SQL results (D-013): aggregates over numeric need CAST; exactness preserved, ergonomics noted.

Founder gate: run `.\demo-sp6.cmd` and `.\demo-sp6-pg16.cmd`.

## SP7 — Chaos (Demos 4 + 7 + failover) · status: DONE (founder ran and watched the full pass on PG17 — kill at 0/F106AD8, fresh-session resume, 23,701-row equality, real failover — 2026-08-17 IST; PG16 passed supervised)

Results (2026-08-17, `just demo-sp7` / `demo-sp7.cmd` — NOTE: the demo crash-restarts the local source instance with `pg_ctl -m immediate` as the failover sim):

| Check | PG 17.6 | PG 16.10 |
|---|---|---|
| Demo 7: decoder killed mid-stream (task abort, dying session); fresh session resumes from the durable ack; marker exactly once; seq contiguous; commit-LSNs monotone | PASS | PASS |
| Registry across sessions: re-emitted Relation metadata deduped (1 version despite 3 sessions) — the self-describing-stream property | PASS | PASS |
| Demo 4: crash after frame-durable BEFORE materialize; rebuild from disk artifacts alone == live source multiset (3 tables) | PASS | PASS |
| Source failover: `-m immediate` crash-restart; session #3 continues from durable ack; post-failover equality + marker exactly once | PASS | PASS |
| One log, three replication sessions, final strict verification | PASS | PASS |

Delivered: `FrameLog::resume` — recovery IS the ack invariant read backwards: scan segments, find the last transaction-complete synced frame, TRUNCATE everything past it (torn tails included), never trust a dying session's tail, continue seq-contiguous. Unit-tested with a deliberately torn frame. Demo driver kills real things: tokio task aborts for the decoder, `pg_ctl -m immediate` for the source.

Self-rating: **8**. Named untested surface:
1. Kill timing is coarse (one kill point per run, post-20-commits); a randomized fault-schedule harness is the W3 machine, post-pilot by ruling.
2. Crash DURING fsync (torn commit frame) simulated only via manual truncation in the unit test, not via OS-level kill.
3. Failover sim restarts the same instance; a real promoted-standby failover (timeline switch!) is untested — timeline handling is a known gap for real deployments.
4. Ops lesson recorded: pg_ctl children must get null stdio or they hold caller pipes hostage (an 11-hour background-task hang taught this; fixed in the demo).

Founder gate: run `.\demo-sp7.cmd` and `.\demo-sp7-pg16.cmd`.

## SP8 — GrayDB Studio + demo runbook · status: BUILT AND EXERCISED LIVE on PG 17.6 — awaiting founder's own run

`just studio` / `.\studio.cmd` → http://127.0.0.1:7432 (axum + one static HTML page, no build chain). Verified by driving the real UI in a browser, not by unit tests:

| Panel / control | Verified live |
|---|---|
| Attach | one click: attach pack + publication + slot with exported snapshot + 32k-row backfill at LSN0 + pump + both shapes materialized |
| Tables | eligibility (full / append-only), per-shape applied LSN, rows visible — updates continuously |
| Replication (graydb.stat_replication) | received_lsn (stream), flushed_lsn (durable ack), applied_lsn, apply lag, frames/commits/spilled |
| SQL editor + classes | eventual · **strong (source barrier — 665 ms, count matched source exactly)** · bounded(Xs) fast-error · target_lsn=… |
| LSN proof footer | on every result: target, received, per-shape applied |
| WAL gauge | walked rung 0 → 1 (57.6%) → 2 (85–113%), 4,481–7,797 frames spilled, recovery to 0.0% |
| Chaos: kill decoder → restart from ack | live; after 300 rows written while dead, strong read = 47,153 = source exactly |
| Chaos: stall log / freeze materialize / crash-restart source | all wired to the SP2/SP4/SP7 mechanisms |
| Event log | real pipeline events with timestamps |

Runbook: `docs/DEMO.md` — 8 beats with exact SQL, what to say, a failure playbook, and an explicit "what NOT to claim" list.

Two real bugs found by driving the GUI (both fixed, and both worth knowing):
1. **`strong` reads could never terminate.** The barrier is `pg_current_wal_lsn()`, but shapes only advance on commit LSNs and the WAL head sits past the last commit (checkpoints, standby snapshots). Fix: track the stream position from keepalives (`IngestMetrics::stream_lsn`) and request replies on the 1s status tick; `strong` now waits for stream ≥ barrier AND applied ≥ durable. D-016.
2. **`received_lsn` was reporting the durable mark**, showing 0/0 right after attach. Corrected to `pg_stat_subscription` semantics (last WAL position received), with the ack position exposed separately as `flushed_lsn`. D-014 naming rule applied.

Self-rating: **7.5** — every panel is real and was exercised end-to-end; the demo has been rehearsed beat by beat. Not 8+ because it has never run in front of a human other than the founder, and Studio is a demo instrument, not a hardened product surface.

Named untested/limited surface:
1. Single-user, no auth, binds 127.0.0.1 only — deliberate (demo instrument, non-goal per charter).
2. Query path rebuilds MemTables per query; fine at demo scale, not a performance claim.
3. `bounded(Xs)` converts a byte lag using a 1 MB/s reference rate (documented in code) — a real staleness clock needs the source heartbeat sampler, which is post-spike.
4. Restart-after-source-crash needs one click (Restart from ack); not automatic reconnection.
5. WAL gauge recovery requires source write activity — honest PostgreSQL slot behavior, surfaced in the UI rather than hidden by writing to the customer's database.

Founder gate: `.\studio.cmd`, then follow `docs/DEMO.md` beat by beat.
