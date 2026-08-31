# GrayDB — Project Documentation v1.0

Consolidates the full design record to date: market evidence, architecture v0.3, six quantitative trials, three ADRs, competitive analysis, adoption/compatibility contract, resource and memory models, risks, and kill gates. Written to serve three uses: engineering reference when building, source material for blog posts, and seed for product documentation. Numbers in this document are either cited from primary sources or produced by our own trials; each trial's assumptions are recorded so results can be reproduced or challenged.

Maturity note (applies globally): every competitor behavior described here is shipped production reality; every GrayDB behavior is designed and sandbox-trialed, not yet built. This asymmetry is stated once here instead of on every line.

---

## 1. Identity and thesis

**GrayDB** (working name): a Postgres-compatible database that starts as an excellent single-node system and becomes a distributed HTAP + search system by flipping per-table/per-tenant switches — never by migration.

One-sentence thesis: **the market punishes distributed-by-default (CRDB/DSQL cost, latency, compatibility tax) and punishes bolt-on distribution (Timescale multi-node, Citus friction), but rewards "start single-node excellent, scale without re-architecture."**

Differentiation thesis in one question: *what does the log carry, and who consumes it?* GrayDB's log is logical row-level with multiple materializer subscribers; every incumbent's log is physical/page-shaped with exactly one consumer.

## 2. Market evidence (research findings)

Sources: HN threads, migration post-mortems, vendor deprecation notices, review sites, industry analyses.

1. **Postgres won; the war moved inside Postgres.** 55.6% developer adoption (up from 33% in 2018); MySQL declining. Databricks bought Neon ~$1B; Snowflake bought Crunchy Data $250M weeks apart. Compatibility is the entry ticket, not a feature.
2. **Write scaling is the loudest unmet ask.** Native HA is DIY (Patroni/Citus assembled by hand); users explicitly request tenant-based horizontal scaling without Citus; single-writer + process-per-connection caps throughput; parallel INSERT patches stalled.
3. **Vacuum/bloat/wraparound: two decades of identical complaints.** Community fix direction is undo-log MVCC (OrioleDB), i.e., replace heap mechanics rather than tune them.
4. **CockroachDB exit pattern:** ZITADEL post-mortem — 3-node CRDB (8CPU/32GB each) cost ~3x a 4CPU/16GB PG with comparable per-region performance; latency and compute overhead from the distribution model; runtime surprises; missing PG functions. G2 consensus: licensing justified only at real scale.
5. **Timescale's retreat is the key instructive datapoint:** multi-node reached ~1% of deployments and was removed; their hypercore table-access-method was deprecated 2.21, sunset 2.22 (Sept 2025). Lesson: bolting distribution or novel storage onto unmodified PG coordination machinery keeps failing.
6. **Operational-simplicity persona is unserved:** developers choose Timescale over ClickHouse explicitly to avoid Keeper/sharding ops ("fits a $5 droplet"). They refuse operational complexity, not performance.
7. **Aurora DSQL = the market's requirements doc:** shipped without FKs, views, triggers, sequences, JSONB, extensions; 10,000-row transaction limit; mandatory client retry loops; confusing DPU pricing. Community verdict: "barely a database." Requirements extracted: real compatibility, no arbitrary transaction limits, predictable pricing.
8. **Sync pipelines are the enemy:** pgvector killed standalone vector DBs; ParadeDB's pitch is same-transaction index updates; pg_duckdb for OLAP. Every eliminated pipeline (app→ES, app→CH, app→Pinecone) is a product category.
9. **Agent-era economics:** >80% of Neon databases created by AI agents; 97% of branches agent-created; requirements are sub-500ms provisioning, copy-on-write branching, scale-to-zero, cost proportional to queries run.

Demand map (frequency × pain): (1) Postgres that scales writes without re-architecture; (2) kill vacuum/bloat/upgrade debt; (3) one engine, four workloads (OLTP+search+analytics+vectors) with single-node-grade ops; (4) pay-for-nothing-idle + instant branching; (5) predictable pricing and honest compatibility.

## 3. Design method

Adversarial by rule: every load-bearing claim must survive (a) a contradiction audit against the rest of the design and (b) a quantitative trial where possible. Failed claims are recorded (Section 11), not deleted. Falsifiable kill gates (Section 16) define Phase-1 success before building. Self-ratings with named weaknesses accompany major outputs.

## 4. Architecture v0.3

Two planes. **Data plane** touches bytes and sits on the latency path; **control plane** decides and never carries data — it publishes maps/policies the data plane reads from cache; nothing in the control plane can block a commit.

### 4.1 Data plane layers

**L1 Compute — real Postgres, stateless.** Actual PG parser/planner/executor/extension API with storage assumptions removed (no local WAL, no checkpoints from compute). Nodes hold caches, not truth; respawn <1s; scale-to-zero; branch-aware. Cost accepted: perpetual rebase on PG majors (3–6 month lag; Neon precedent).

**L2 Shared log — the one hard thing we build.** Replicated, ordered, **logical row-level** WAL service. Commit = quorum append (2 of 3, cross-AZ). All consensus in the system lives here; compute and materializers run none. Single-writer PG semantics per database by default. Interface: `append(batch) → LSN`, `tail(from_LSN)`, `read(range)`. Records carry relation OID (~4 bytes) — tables are multiplexed onto one stream per database at stage 0. Throughput ceiling per stream is bandwidth (~500K txn/s @ 0.5KB/txn), not IOPS.

**L3 Materializers — one log, three read shapes; all derived, all rebuildable.**
- *Row store:* dual-engine. Stock **heap is default** (full extension + index-AM surface: GIN, GiST, SP-GiST, BRIN, PostGIS, pg_trgm, pgvector). **Undo-MVCC engine** opt-in per table (`WITH (engine='undo')`) for high-churn B-tree workloads: no vacuum, no wraparound; GIN/GiST on undo is a multi-year roadmap item, stated as such. Undo failure mode: `snapshot too old` when a long transaction outlives `undo_retention_target` (default 1h) — an Oracle-style trade replacing bloat, disk-bounded not RAM-bounded.
- *Columnar:* aged log segments compacted into compressed columnar files (parquet-class) on object storage with delete bitmaps; executed via **embedded DataFusion** behind vectorized custom scan nodes with filter/agg pushdown; PG executor handles joins/stitching above. Materialization is threshold-or-explicit (auto above ~1GB table size or `SET (columnar = on)`); minimum segment size prevents small-file explosion.
- *Search:* Tantivy BM25 + HNSW vector indexes fed from the log in commit order; SQL predicate `@@@` plans like any index.
- Rule: materializers are disposable; recovery = snapshot + delta replay (pure replay rejected by T4 RTO math). PITR, branching, replica creation are the same primitive (log position + snapshots).

**L4 Object storage.** Cold segments, snapshots, branch DAG on S3. Branch = metadata fork (copy-on-write). Scale-to-zero database costs its S3 footprint.

### 4.2 Control plane

- **Catalog:** schemas, branch DAG, ownership + placement maps; versioned-map pub/sub; DDL pauses if catalog down, DML continues on cached maps.
- **Governor:** hot-key detection (abort-spike triggered), ownership escalation/de-escalation, `graydb.hot_keys` view, manual pin/unpin.
- **Autoscaler:** compute count, materializer pool sizing, placement.

### 4.3 Component contracts

| Component | Owns (state) | Interface | Scales by | On failure |
|---|---|---|---|---|
| PG compute | nothing durable | PG wire in; log append + LSN reads out | node count; scale-to-zero | respawn <1s; zero loss |
| Shared log | the truth (ordered records) | append/tail/read | streams (stage 1), shards (stage 2) | quorum masks 1 node/AZ; leader re-election ~s |
| Row store | derived heap/undo pages | tuple reads at snapshot LSN | partitions + parallel apply | snapshot+delta replay (~min) |
| Columnar | derived segments + delete bitmaps | DataFusion scans, visibility-filtered | stateless scan-out over S3 | disposable; re-compact |
| Search | derived Tantivy/HNSW segments | SQL predicates at applied-LSN | indexing workers per stream | disposable; replay |
| Object storage | cold truth | get/put immutable | S3 | provider durability |
| Catalog | maps + branch DAG | versioned pub/sub | tiny replicated KV | data plane runs on cache |
| Governor | hot-key set | telemetry in; ownership out | per-tenant workers | escalations freeze; routing keeps working |

## 5. Concurrency and progressive scaling

- **Stage 0 (default):** one writer, elastic readers, full PG semantics, no consensus in read path, no retry requirements. Most users live here forever, without vanilla ceilings (commit-is-append, elastic reads, no vacuum debt on undo tables).
- **Stage 1 (opt-in, per tenant):** log stream per tenant; write scale-out with zero cross-tenant coordination. Answer to "scale by tenant_id without Citus."
- **Stage 2 (opt-in, per table):** `ALTER TABLE ... SET DISTRIBUTED BY (key)`. **Owner-first execution** (design inverted by Trial R3): hot keys assigned owners that serialize read-modify-writes in owner memory (~30µs apply) and group-append to the log; **OCC is the fast path for the cold tail**, not the primary mechanism. Contention governor escalates on abort-rate spikes with <100ms handover. Cross-owner multi-key transactions: ordered owner acquisition (deadlock-free by global key order) + one atomic group append — protocol is open risk #1, latency stacks with owner count (untrialed).
- **Physical limits, published:** OCC per-key ceiling ~30–50 writes/s; owner-serialized ceiling ~33K writes/s/key; beyond that only commutative batching or key-splitting. Error surface: distributed tables emit `SQLSTATE 40001` (same retryable class PG emits under SERIALIZABLE) — no novel error taxonomy.

## 6. Consistency model

LSN is the universal spine. Every query carries a snapshot LSN. Row-store reads serve it directly. Columnar/search honor per-query classes: `SET graydb.consistency = 'strong'` (wait until applied-LSN ≥ snapshot-LSN) | `bounded(X ms)` (error fast if staleness exceeds X) | `eventual` (never blocks). Same LSN-token mechanism provides read-your-writes on read replicas. Freshness observable at `graydb.freshness` (per stream × per shape).

## 7. Memory management architecture

Rule 0: every byte accounted; kernel OOM-killer firing is a GrayDB bug by definition. Four pool classes per node — **System, Cache, Work, Apply** — owned by a per-node **memory broker**; cgroup v2 `memory.max` backstop 8% above broker total.

Reference budgets:

| Pool | Compute 64GB | Row-store 128GB | Columnar+Search 64GB | Log 32GB |
|---|---|---|---|---|
| System | 6 | 8 | 6 | 4 |
| Cache | shared_buffers 6 (hot set; bulk = NVMe LFC direct-IO, not RAM) | page cache 92, O_DIRECT + huge pages | segment cache 24 (Tantivy mmap + parquet footers) | tail cache 20 (subscribers catch up from RAM) |
| Work | 36 query-grant pool (PG operators + DataFusion draw from same pool) | 6 (compaction, snapshots) | 22 (scans, merges; spillable) | 2 (append/group-commit rings) |
| Apply (floor, inviolable) | — | **12 reserved** | **8 reserved** | 2 in-flight quorum buffers |
| Connections | ~400 pooled × 10MB + relcache pool | — | — | — |

**Grant protocol:** plan → estimate → `request_grant(bytes)` → `granted | granted_with_spill | queued(timeout 5s)`. Hierarchical leases (node→class→query), freed on end, leak-audited. Exceeding grant ⇒ spill to NVMe temp, never expansion. Kills PG's unbounded `work_mem × backends × operators` failure (200 conns × 5 hash joins × 64MB = 62GB legal on a 64GB box); `work_mem` becomes a hint, grant is binding. DataFusion's FairSpillPool wired to same broker. Precedents: SQL Server grants, Greenplum statement_mem; vanilla PG has nothing equivalent.

**Second inherited bug fixed:** relcache/catcache have no eviction upstream; capped per backend (`graydb.relcache_cap`, default 32MB, LRU).

**Bounded-by-construction states:** undo recent buffers ~4GB RAM (retention is disk/time-bounded → `snapshot too old`, never RAM-bounded). Owner working set = last committed value + uncommitted batch per owned key (10K keys ≈ 40MB; cap `owner_working_set` 512MB, evict cold keys to router) — cannot grow unbounded even under attack.

**Degradation ladder (exact order):** 1) operator spill (+latency, query finishes) → 2) adaptive cache eviction by marginal hit-rate → 3) admission queue (new grants wait) → 4) cancel largest-grant query, `SQLSTATE 53200` + peak-usage detail → 5) kernel `memory.high` reclaim stall = alert, should never happen. **Apply floor never shrinks** (query pressure cannot grow freshness lag / break strong-read SLOs); reverse-pressure: apply saturation signals gateway to slow *write* admission. Memory pressure converts to bounded latency, never divergence.

**OS substrate:** cgroup v2 per service; swap off; THP madvise; huge pages for big caches; jemalloc (Rust, stats exported) + `MALLOC_ARENA_MAX=2` (PG); NUMA: append rings pinned, caches interleaved; io_uring + O_DIRECT on row store.

**Observability:** `graydb.memory` (node, pool, budget, used, granted, queued, spills/s, evictions/s, apply_floor_ok); per-query `peak_mem`, `spilled_bytes` in pg_stat_statements; `EXPLAIN (ANALYZE, MEMORY)`.

**Residuals (honest):** process-per-connection floor inherited (~10MB×conns; pooler mitigates); spill is a predictable latency cliff, not a fix; Tantivy mmap accounting is an estimate; mixed allocators (glibc+jemalloc) need a fragmentation soak test; all budgets designed, not measured.

## 8. Resource model — engines, tables, processes

Engine ≠ process. Engines are table access methods: function-pointer dispatch inside the existing backend (PG 12+ TAM mechanism; OrioleDB precedent). 1 connection = 1 backend (unchanged); 1 engine = shared library + fixed per-node worker pool; 1 table = catalog rows + cache entries (KB), never a process. 2000 tables = same process count as 20.

| Resource | Scales with | Cost each | 2000-table schema |
|---|---|---|---|
| Backends | connections | 5–10MB | unchanged; pooler-governed |
| Engine fixed cost | engines/node (max 2) | undo: 256MB–1GB shared undo buffer + 2–4 bg workers, once per node | identical for 1 or 1,999 undo tables |
| Catalog | tables | 2–5KB | ~10MB total |
| Relcache | tables touched per backend | 5–15KB/table | all-2000-touched ≈ 10–30MB × pooled conns — the number to watch (same as vanilla PG) |
| Log streams | databases (stage 0) | MBs state | **1 stream, not 2000** (OID multiplexing) |
| Apply workers | stream MB/s | pool, hash-partitioned by OID | table count irrelevant |
| Columnar catalogs | tables with columnar enabled | KB–MB + S3 objects | policy-gated (threshold/explicit) |
| Search writers | declared indexes, active only | 64–256MB while ingesting, ~0 idle | declare 5–50, not 2000 |

Node cost = base_PG + Σ_engines fixed + Σ_tables KB + Σ_active_search_writers RAM. Engines are O(1)/node; tables O(n) in KB; nothing is O(tables×engines). Known bites at 2000 tables: columnar small-file problem (solved by threshold + min segment size); heap tables keep PG per-table background costs (autovacuum scheduler iteration, 3 files/table + per-index ⇒ ~10–14K handles, LRU-managed — unchanged, not worsened; undo tables exit autovacuum and consolidate files). Monitoring cardinality: freshness is per stream × shape, not per table. Very wide schemas (100K-table multi-tenant): relcache × connections is the real ceiling on any PG; GrayDB's answer is stage 1 (tenant streams + schema-per-tenant), not a node fix.

## 9. Trials — methods, assumptions, results

**T1 Commit latency** (Monte Carlo, 200K samples; local fsync lognormal median 0.30ms + 0.2% stalls; cross-AZ one-way 0.35ms; quorum 2-of-3): local p50/p95/p99 = 0.30/0.64/0.89ms; GrayDB 0.93/1.18/1.30ms. **p50 regression 3.1x (range 2–4x by topology); p99 ratio 1.46x in GrayDB's favor** (stall masking). Published, not hidden. Throughput ceiling per stream ~500K txn/s @0.5KB (bandwidth-capped).

**T2 OCC abort vs contention** (4 writes/txn, 5ms window, 1M keys): uniform 0.004% @500tps → 0.4% @50K. Zipf a=0.8 (top key 1.3% traffic): 1.56% @500 → **23.4% @20K**. a=1.01 (top ~6.9%): **79.5% @20K**. a=1.2 (~24%): **98.2% @20K**. Finding: OCC cliffs under skew; does not degrade gracefully.

**T3 Materializer replay lag** (burst 40→400 MB/s for 120s): at 200 MB/s apply, staleness peaks **120s**; strong reads blocked ~4.5 min total. Indexing costs 3–10x raw append. Consequence: consistency classes mandatory + elastic burst capacity.

**T4 Rebuild RTO** (0.4 GB/s/node apply): pure replay 1TB = 43min (1 node) / 5.3min (8); 10TB = 7.1h / 53min. Snapshot + 5%-churn delta = minutes at any size. Consequence: snapshots stay; replay remains the unifying primitive.

**T5 Engine bake-off** (DuckDB 1.5.5 vs DataFusion 54; 8M-row parquet, 16 row groups, 1 core, identical SQL, warm medians): full-scan agg 0.14 vs 0.37s (DuckDB 2.7x); join+agg 2.1x; time-pruned agg 0.45x (DataFusion faster); selective needle 14x DuckDB. **Money finding: with MVCC visibility predicates (every GrayDB query), gap collapses to 1.26x (0.32 vs 0.40s)** — visibility overhead 2.29x (DuckDB) vs 1.07x (DataFusion). Concurrency 4× on 1 core favored DataFusion (0.39 vs 0.93s wall) — partially harness artifact (per-connection setup), flagged. Limitations: 1 core, page-cached, Python bindings, 3 reps.

**T6 (R3) Contention governor**: (a) ceilings — OCC 22.1% aborts @50 writes/s/key, 63.2% @200; owner M/D/1 stable to ~33K/s/key (135µs wait at ρ=0.9), ~1000x OCC ceiling, ~1ms p50 including group append. (b) steady state @20K tps, θ=20/s — a=0.8: 144 keys (0.014% keyspace, 12.1% traffic) escalated, aborts 23.4→**2.63%**, 40% txns on owner path; a=1.01: 280 keys (44.9% traffic), 79.5→**4.74%**, **90.7% owner-path**; a=1.2: 251 keys (74.6% traffic), 98.2→**6.53%**, **99.6% owner-path**. **Inversion finding: under real skew the system is owner-serialized with OCC cold tail — stage 2 designed owner-first.** (c) flash-sale transient (1→3,000 writes/s step): escalation ~100–110ms after step; **~780–900 aborted/retried txns** before rescue; retry storm peaks **5x** offered load. Levers: abort-spike triggering, <100ms handover, client backoff.

## 10. Architecture Decision Records

**ADR-001 — Skeleton: shared-log disaggregation + real PG compute.** Rejected: distributed-first with faked PG (compatibility long tail + always-on tax; CRDB/DSQL evidence); stock-PG extension (Timescale multi-node/TAM post-mortems). Consequence: perpetual rebase cost accepted.

**ADR-002 — Columnar execution: embed DataFusion.** "Build own" killed (two mature engines within ~3x; negative-sum race). DuckDB rejected on fit despite raw-speed win: measured GrayDB-shaped gap ~1.26x is below the threshold to override Rust-native TableProvider integration (scan-level MVCC), in-process memory/scheduling control (broker unification), Apache governance, direct precedent (InfluxDB 3.0, GreptimeDB). Revisit trigger: >3x gap on visibility-filtered TPC-H-derived suite at 8+ cores, or MVCC pushdown overhead >1.3x.

**ADR-003 — Stage-2 concurrency: owner-first deterministic execution, OCC cold tail.** Forced by T2 + T6. Open: cross-owner ordered-acquisition protocol.

## 11. Contradiction audit record (v0.1 → v0.3)

- **C1** "Transactionally consistent search" vs async materializer → resolved by consistency classes (strong pays replay lag; T3).
- **C2** "Extensions day one" vs undo engine (index AMs heap-TID-coupled; OrioleDB precedent) → dual-engine, heap default.
- **C3** "Solves write scaling" half-true: log offload removes I/O, not single-writer CPU execution ceiling → parallel write apply scoped as separate project.
- **C4** Commit latency regression unstated → published spec line (T1).
- **C5** Stage-2 OCC = DSQL retry loops → confirmed by T2; redesigned owner-first (T6).
- **C6** "No backups, just replay" fails RTO math (T4) → snapshot + delta.
- **C7** "Planner stitches transparently" hid Volcano tuple-at-a-time executor problem → embed DataFusion (ADR-002).

## 12. Competitive analysis

Organizing question: what does the log carry, and who consumes it?

**Family A — single-node PG lineage (PG/RDS, Timescale, OrioleDB, ParadeDB):** physical page-oriented WAL, one consumer (crash recovery/physical replicas); every add-on inherits heap + physical WAL. Timescale compresses by rewriting heap chunks in place (why compressed DML was restricted; why their TAM died). ParadeDB grafts LSM-shaped index onto page-shaped durability. OrioleDB fixes MVCC but patches core, single-node. Concession: local-fsync PG beats us 2–4x on single-connection commit p50 — chatty sequential single-row committers should stay on vanilla PG.

**Family B — disaggregated PG (Aurora, Neon), the decisive comparison:** Aurora ships physical redo to a 6-way fleet (4/6 quorum); storage can only materialize heap pages; analytics answer is zero-ETL to Redshift (a rebuilt pipeline product, different engine/semantics/lag); **vacuum still exists**. Neon: WAL → 3 safekeepers (Paxos; structurally our log) → pageserver builds copy-on-write page store on S3 (branching/PITR as metadata) — closest cousin; payload is physical WAL; one log, **one read shape**; vacuum remains. **Moat claim (physics, not features): they disaggregated the durability of the heap; we disaggregate the meaning of the write.** Logical row-level records materialize as heap, undo, columnar, or search shapes on one LSN spine; retrofitting that means rebuilding the layer that *is* their product. Not claimable against them: branching, scale-to-zero, PITR — table stakes vs Family B.

**Family C — distributed SQL (CRDB, Yugabyte, DSQL):** consensus per range/tablet; everyone pays distributed physics always. CRDB contended row = leaseholder serialization *through Raft* per write; our owner path = in-memory serialization (~30µs) + amortized group append (measured ~33K/s/key, ~1ms p50); our cold tail pays zero per-key coordination. **Yugabyte deep-dive (v0.4 addendum, verified):** YSQL = the actual PG upper half (PG 15.2 lineage after a multi-year rebase from a PG 11 base — the fork tax observed at decade scale, vindicating WL4) over DocDB, an LSM document store with one Raft group per tablet; single-shard fast path exists because the distributed tax is real (convergent with T2/T6); MVCC = hybrid-timestamp versions GC'd at compaction, no VACUUM (the LSM road to the goal our undo engine targeted; the wedge's immutable segments sidestep the question entirely); extension ceiling = upper-half-only, storage-touching AMs must be reimplemented (C2 lived in production). No second read shape: their analytics/search answer is CDC out via Debezium/Kafka — Yugabyte customers have the wedge's target problem. Since v2024.1.1 they ship PG-compatible logical replication (pgoutput bundled; replica identity CHANGE/INDEX unsupported; "LSN" is a facade over hybrid time anchored to a consistent_point) → Yugabyte is a plausible **GrayDB source** post-W1 behind a compatibility spike (WL2 event-trigger pack needs YSQL validation), extending TAM to PG-compatible systems of record. DSQL: OCC everywhere + retries + row limits + thin surface = our requirements doc. Concession: genuine multi-region active-active with region-loss survival → Family C/Spanner is correct; permanent non-goal. Flag: CRDB per-hot-key ceiling asserted from mechanism reasoning, not measured — needs a real benchmark before external use.

**Family D — HTAP/specialists:** TiDB+TiFlash validates the thesis (columnar as Raft-learner log subscriber, shipped) — differences: MySQL surface, per-region consensus tax, separate replica fleet vs S3-native compaction, no search shape. SingleStore: unified row+column, excellent execution; proprietary surface, cluster-first ops. ClickHouse: king of pure OLAP scan; we remove the *pipeline* below warehouse scale, we don't out-scan it. Elasticsearch: the CDC pipeline (Debezium/Kafka/mapping drift/reindex weekends) is the product we delete; LSN `strong` search reads are architecturally unavailable to an external cluster.

**Mechanism scoreboard:**

| System | Commit point | Consensus scope | Log payload | 2nd read shape | Vacuum | PG extensions | Day-1 distribution tax |
|---|---|---|---|---|---|---|---|
| PG/RDS | local fsync | none | physical | none | yes | full | n/a |
| Aurora | 4/6 storage quorum | storage fleet | physical redo | zero-ETL→Redshift | yes | full | none |
| Neon | 3-safekeeper quorum | log only | physical WAL | none | yes | near-full | none |
| CRDB | per-range Raft | every range | KV/MVCC | none | no (GC TTL) | none | always |
| Yugabyte | per-tablet Raft | every tablet | DocDB | none | no | partial | always |
| DSQL | journal+adjudicator | commit-time | logical-ish | none | no | minimal | always (OCC) |
| TiDB+TiFlash | per-region Raft | every region | Raft log | columnar learner | no | none (MySQL) | always |
| **GrayDB** | **2/3 log quorum** | **log only** | **logical row** | **columnar+search, same log** | **no (undo opt-in)** | **full (heap)** | **zero until opted in** |

**Buying triggers:** (1) pre-sharding moment — RDS writer nearing biggest box; buys absence of re-architecture (stage 1 behind a flag vs Citus/app-sharding project). (2) pipeline tax — PG+ES+CH+Kafka teams; buys deletion of 2 systems + all pipelines; measurable as 1–2 engineers of permanent load. (3) vacuum refugees — high-churn OLTP; buys `engine='undo'` on the five painful tables; POC = gate G4 in a week. (4) agent platforms — buys per-branch analytics+search that Neon branches lack. **Adoption wedge:** sidecar mode — attach to existing RDS/PG via logical replication as the analytics+search replica; zero migration; promote later; exit hatch both directions.

**Where we lose, on purpose:** global active-active (Spanner/CRDB); sub-0.5ms single-connection commit (local NVMe PG); petabyte pure OLAP (CH/BigQuery/Snowflake); decade-track-record procurement (Aurora); tiny apps (RDS is fine).

## 13. Adoption and compatibility contract

**Works unchanged:** psql/DBeaver/DataGrip; Hibernate/Django/Prisma/SQLAlchemy/ActiveRecord (no dialect change); pg_dump/pg_restore/COPY; logical replication in *and* out (in = wedge; out = anti-lock-in exit); poolers (less necessary — gateway pools; compute still process-per-connection); EXPLAIN/pg_stat_statements/pg_stat_activity (new nodes: `ColumnarScan (datafusion)`, `SearchScan (bm25)`); all extensions on heap tables.
**Partial:** extensions on undo tables (B-tree only; GIN/GiST = years).
**Gone (replaced):** pgBackRest/wal-g (→ log snapshots + branch-at-LSN PITR); pg_upgrade (→ rolling compute swap; we carry 3–6mo PG-major lag); physical standby to external vanilla PG (logical out only); pg_repack/manual vacuum tuning on undo tables (the point).

**Developer delta ≈ zero:** connection/auth/drivers unchanged; stage-0 errors identical; distributed tables emit `SQLSTATE 40001` (retry wrapper devs should already have); commit p50 0.9 vs 0.3ms visible only to chatty sequential loops (fix = batch/pipeline; same as Aurora guidance); hot keys need no app code (aborts → ~1ms queue latency).

**Entire new SQL surface (six constructs):** `WITH (engine='undo')`; `ALTER TABLE ... SET DISTRIBUTED BY (key)`; `SET graydb.consistency = strong|bounded(X)|eventual`; `WHERE col @@@ 'query'` (BM25); `CREATE/DROP/PROMOTE BRANCH ... AT LSN`; `graydb.freshness` view. Replaces conservatively 40+ concepts (ES DSL/mappings/ILM/analyzers; CH dialect/MergeTree/Keeper; Debezium/Kafka/schema registry).

**DBA runbook diff (highlights):** vacuum class deleted on undo tables (new watch: undo retention vs long txns); replica-lag monitoring → one LSN gauge per shape; failover drill = kill writer, <1s respawn, boring; PITR = branch-at-LSN, cheap enough to drill monthly; schema migrations tested on a branch of prod data before promote; reindex of search/columnar = drop + replay, no lock; capacity = four dials (compute/log streams/materializer pool/S3) vs one instance-size dial — more control and more sizing decisions; hot-key incident = governor auto + `graydb.hot_keys` + pin/unpin.

**Complexity ledger:** added ~6 failure modes (snapshot-too-old; strong-read blocking; ~100ms handover blip w/ ~800 retries; 2-of-3 quorum loss = write outage; branch sprawl → S3 creep; PG version lag) vs removed ~7 classes (wraparound emergencies; silent ES/CH staleness; app hot-row firefighting; single-node total loss; forgotten backups; pg_upgrade weekends; pipeline consistency incidents). Net for a senior PG DBA: 3 new mental models (LSN-as-freshness; materializer sizing at 3–10x write CPU; undo retention) for 4 deleted.

**Sidecar adoption path (each step reversible):** 1) `CREATE PUBLICATION` on existing primary → 2) GrayDB subscribes; builds columnar+search → 3) point analytics/search reads at GrayDB (risk = a read replica) → 4) weeks of freshness + result-diff comparison vs ES/CH → 5) promote with reverse replication as rollback window → 6) rollback = repoint reads / flip replication.

## 14. Failure matrix and capacity envelope

| Failure | Blast radius | Recovery | Data loss |
|---|---|---|---|
| Writer compute dies | open txns abort | respawn+reconnect ~1s | none |
| One log node / full AZ | none (quorum holds) | background re-replication | none |
| Two log nodes | writes unavailable; reads at last LSN | restore quorum (honest worst case) | none committed |
| Materializer crash | that shape stale/unavailable per class | snapshot+delta replay, minutes | none (derived) |
| Materializer lag (burst) | strong slow, bounded errors, eventual fine | elastic apply capacity | none |
| Control plane down | no DDL/escalations/scaling | data plane on cached maps indefinitely | none |
| Hot key erupts | ~800 retried txns in ~100ms | governor + client backoff | none |

**Capacity envelope (all self-measured):** commit p50 ~0.9ms (2–4x local, better p99 tails); ~500K txn/s per log stream; ~33K writes/s per owned key; ~50/s per key on OCC path; columnar per-core within 1.26x of DuckDB on visibility-filtered scans; any-materializer rebuild in minutes via snapshot+delta.

**Deployment:** 3 AZ; one log replica per AZ (commit = any 2); writer compute AZ-a, readers AZ-b, materializer pool AZ-c (placement-free, respawn anywhere); S3 shared. Losing a full zone loses no data and no availability.

## 15. Risk register (live)

| # | Risk | Severity | Status |
|---|---|---|---|
| 1 | Cross-owner transaction protocol (multi-hot-key) | High | undesigned; next trial target |
| 2 | PG rebase burden per major | High | accepted; budget it (Neon precedent) |
| 3 | DataFusion wide-scan gap | Med | bounded 1.26–2.7x/core; revisit trigger set |
| 4 | Undo space amplification under churn | Med | gate G4 |
| 5 | Materializer freshness under sustained overload | Med | needs sizing model (not just burst) |
| 6 | Commit-latency regression on chatty ORMs | Med | publish spec; batching guidance |
| 7 | Single-stream bandwidth ceiling (mega-tenant) | Low | stage 1/2 path exists |
| 8 | Memory broker under adversarial mix (many small grants + one giant + apply burst) | Med | untrialed; simulable |
| 9 | Maturity asymmetry vs incumbents | High (commercial) | sidecar mode amortizes; only time cures |

## 16. Phase-1 kill gates (falsifiable)

- **G1:** TPC-C-like OLTP on dual-engine ≥0.9x vanilla PG throughput, p99 ≤1.5x.
- **G2:** hybrid row+columnar ≥10x vanilla PG on cold aggregates with <10% pure-OLTP regression.
- **G3:** strong-consistency search p99 ≤150ms at 2x provisioned burst.
- **G4:** undo engine survives 72h @50% update churn with <5% space amplification.

## 17. Build phasing

- **Phase 1 (single-node wedge, independently sellable):** PG compute + log-structured storage + undo engine (opt-in) + DataFusion columnar + Tantivy search. Pitch: "Postgres without vacuum, with built-in analytics and search."
- **Phase 2 (disaggregation):** log service, S3 tiering, branching, scale-to-zero, elastic readers, consistency classes.
- **Phase 3 (progressive distribution):** per-tenant streams; owner-first per-table sharding + governor.

## 18. Open questions, ranked

1. Cross-owner protocol trial: latency stacking + abort behavior for 2–4-owner transactions.
2. Memory-broker adversarial-mix trial (risk #8).
3. Parallel write apply on single-writer path (closes C3 fully).
4. Materializer capacity sizing model (steady-state overload).
5. `graydb.*` observability schema spec (views/columns — where "one telemetry plane" becomes real).
6. Log service low-level spec: record format + append/tail API (everything programs against it).
7. Rebase strategy: patch-set budget, upstream-contribution policy.
8. Pricing model that survives DSQL-style community audit.
9. CRDB hot-key comparative benchmark (validate or soften the per-key claim).

## 19. Using this document

Blog-post seeds ready to extract: "What does your database's log carry?" (Section 12 moat thesis); "We tried OCC and measured the cliff" (T2/T6); "DuckDB vs DataFusion when every query carries MVCC" (T5); "Killing work_mem: memory grants for Postgres" (Section 7); "The contradiction audit: designing by trying to kill your own design" (Sections 3, 11). Product-doc seeds: Sections 6 (consistency), 13 (compatibility contract, new SQL surface, sidecar guide), 14 (failure matrix). Everything in this document is versioned against architecture v0.3; changes go through the same audit + trial method.
