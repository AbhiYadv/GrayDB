# GrayDB Memory Architecture — v0.1 (deep specification)

Scope: every memory subsystem in the system, at mechanism level. Method: start from the complete PostgreSQL memory inventory (nothing skipped), assign each subsystem a disposition — INHERIT, MODIFY, REPLACE, DELETE, or NEW — then specify the new machinery, per-node budgets with derivations, the broker protocol, the full parameter surface, pathological scenarios, and finally the LOCK LIST: the decisions we freeze for Phase 1 with revisit triggers. Supersedes the Chapter-7 summary in the docs site (that chapter becomes the overview; this is the spec behind it).

Maturity: designed, not measured. Every default below is a locked starting value with a stated revisit trigger, not a benchmarked optimum.

---

## Part 1 — Complete inventory: every PG memory subsystem, dispositioned

The rule for this table: if PostgreSQL documentation or source has a named memory mechanism, it appears here. No shortcuts.

| # | PG subsystem | What it is in PG | GrayDB disposition | Detail |
|---|---|---|---|---|
| 1 | Memory contexts (`palloc` hierarchy: AllocSet, Generation, Slab, Bump) | Region-based allocator; every backend allocation lives in a context tree (TopMemoryContext → per-query → per-tuple); freed by subtree reset, which is why PG rarely leaks | **INHERIT + instrument** | Contexts are excellent; we keep them untouched for correctness. Addition: a per-backend accounting hook — context create/extend callbacks increment a per-backend atomic counter that rolls up to the broker's Connection quota. Cost: one atomic add per malloc'd *block* (not per palloc chunk — blocks are 8KB→8MB doubling), measured-negligible by design. |
| 2 | `shared_buffers` + buffer manager (clock-sweep, usage counts, buffer pins, ring buffers for seq-scan/VACUUM/COPY) | The page cache for heap/index pages; 8KB slots in shared memory; strategy rings prevent one scan evicting the world | **MODIFY (compute) / REPLACE (row-store node)** | Compute: keep a small `shared_buffers` (default 2–6GB) for catalog pages + hottest working set; bulk caching moves to the **LFC** (item 26). Ring-buffer strategies inherited unchanged. Row-store node: PG buffer manager replaced by our page cache (Part 2B) — O_DIRECT, 2Q-style replacement, broker-owned. |
| 3 | `work_mem` (per sort/hash/agg operator, per backend; hash spill since v13; `hash_mem_multiplier`) | The famous footgun: per-operator, per-backend, no global cap | **REPLACE semantics, keep knob** | `work_mem` becomes a per-operator *planning hint* only. The binding limit is the query grant (Part 3). `hash_mem_multiplier` folds into grant estimation. Plans still show per-node estimates; execution enforces the grant. |
| 4 | `maintenance_work_mem` (index build, VACUUM dead-TID store) | Larger per-op budget for maintenance | **REPLACE** | Maintenance ops request grants from a dedicated **Maintenance sub-class** of Work (default 15% of Work, preemptible by queries). Index builds spill like everything else. |
| 5 | `autovacuum_work_mem` + autovacuum worker memory | Per-worker dead-TID memory | **INHERIT (heap tables only)** | Heap tables keep autovacuum, unchanged behavior, but worker memory draws from the Maintenance sub-class so N workers can't stack silently. Undo tables: subsystem does not apply. |
| 6 | `temp_buffers` (session-local buffers for temp tables) | Per-backend, allocated lazily, default 8MB | **INHERIT** | Counted against the backend's Connection quota. Temp-table-heavy sessions become visible instead of invisible. |
| 7 | `wal_buffers` (default: 1/32 of shared_buffers, capped at one WAL segment) | Staging for WAL before fsync | **DELETE (compute) → NEW: append rings** | No local WAL exists. Replaced by per-stream **append rings**: lock-free MPSC buffers where backends deposit logical records for group commit. Sizing = bandwidth-delay product (Part 2D). |
| 8 | CLOG/pg_xact + SLRU caches (xact status, commit_ts, multixact, subtrans, notify, serializable) — sizes configurable in modern PG | Tiny shared caches over on-disk status arrays; visibility checks hit them constantly | **INHERIT (compute + row-store)** | Heap MVCC still needs xid status; undo engine consults xact status during undo traversal too. Keep PG's SLRUs and their GUCs as-is. GrayDB addition: SLRU pages are broker-accounted under System (they're small; accounting is for visibility, not control). |
| 9 | relcache / catcache / plancache (per-backend, **no eviction in PG**) | Metadata caches; the silent killer on wide schemas × many connections | **MODIFY: add eviction** | `graydb.relcache_cap` (default 32MB/backend, LRU on relcache entries; catcache capped proportionally; plancache entries LRU with generic-plan pinning respected). Eviction never touches entries with open cursors/portals. This is upstream-divergent surgery — flagged as a rebase-cost item (risk #2). |
| 10 | Prepared statements / portals / cursors | Live in per-backend contexts | **INHERIT** | Counted in Connection quota; `graydb.stat_activity_mem` exposes per-backend totals so "10K prepared statements" is diagnosable. |
| 11 | DSM/DSA + `shm_mq` (parallel query shared memory, parallel hash) | Dynamic shared segments for parallel workers | **INHERIT + grant-integrate** | A parallel query's grant covers leader + all workers; DSA allocations debit the same grant. Worker startup denied if grant can't extend — query falls back to lower parallelism (planner already handles degradation). |
| 12 | Logical decoding: ReorderBuffer + `logical_decoding_work_mem` (default 64MB, spills/streams beyond) | Reassembles transactions from WAL for logical rep | **INHERIT (outbound) / N/A (inbound)** | Outbound logical replication (the sidecar exit hatch) keeps PG's machinery, grant-accounted under Work. Inbound doesn't exist as PG code — our materializer apply pipeline (Part 2B/2C) is its replacement and lives in the Apply pool. |
| 13 | JIT (LLVM) per-query memory | Compilation arenas | **MODIFY: off by default** | DataFusion owns analytical execution; PG-side JIT adds RAM variance for little gain in our split. `jit=off` default; enabling adds a fixed surcharge to the query's grant. |
| 14 | Backend process anatomy (~5–10MB floor: stack, heap, catalog snapshot) | The process-per-connection cost | **INHERIT (honest residual)** | Floor mitigated by gateway transaction pooling (~400 backend cap default per compute node), not deleted. |
| 15 | `effective_cache_size` (planner-only hint) | Tells the planner how much OS+shared cache exists | **MODIFY: auto-computed** | Auto-set to shared_buffers + LFC size + (for columnar plans) segment-cache size. Manual override retained. |
| 16 | `shared_memory_type`, `huge_pages`, startup shared segment layout | mmap segment at postmaster start | **INHERIT + lock huge pages** | `huge_pages=on` (not `try`) for shared_buffers and row-store page cache — fail loudly at startup rather than silently degrade TLB behavior. Explicit 2MB-page sizing math in Part 5. |
| 17 | Local buffers for `CREATE TEMP TABLE` I/O, `logical_replication_mode` buffers, misc per-backend arrays | Small per-backend allocations | **INHERIT** | Connection quota. |
| 18 | `vacuum_buffer_usage_limit` / strategy ring sizes | Caps vacuum's buffer-cache footprint | **INHERIT** | Heap-only; defaults unchanged. |
| 19 | pg_stat / cumulative stats shared memory (shared-memory stats in modern PG) | Stats collector state | **INHERIT** | System pool. |
| 20 | `max_locks_per_transaction` lock table, predicate lock memory (SSI) | Shared lock tables sized at startup | **INHERIT** | System pool; sized formula documented (locks: `max_locks_per_transaction × (max_connections + max_prepared_transactions)` entries ≈ 100B each). |
| 21 | Checkpointer/bgwriter working memory | Flush bookkeeping | **DELETE (compute)** | No checkpoints from compute — the subsystem does not exist. Row-store node has its own flusher (Part 2B) in Apply. |
| 22 | Base backup / WAL sender buffers | Streaming replication memory | **DELETE (physical) / INHERIT (logical)** | No physical standbys exist. Logical walsender inherited (item 12). |
| 23 | **NEW — Log-append client rings** (replaces 7) | — | **NEW** | Part 2D. |
| 24 | **NEW — LFC: Local File Cache on compute NVMe** | — | **NEW** | Neon-precedent design: caches row-store pages on compute-local NVMe with a small RAM index. RAM cost = index only (~100B/chunk); capacity = NVMe, not RAM. Part 2A. |
| 25 | **NEW — Row-store page cache** (replaces 2 on that node) | — | **NEW** | Part 2B. |
| 26 | **NEW — Undo buffers + retention accounting** | — | **NEW** | Part 2B. |
| 27 | **NEW — Apply pipelines (decode → route → per-partition apply)** | — | **NEW** | Part 2B/2C; the Apply pool's tenant. |
| 28 | **NEW — DataFusion MemoryPool integration** | — | **NEW** | Part 2C: our broker implements DataFusion's `MemoryPool` trait; every operator consumer registers, `try_grow` maps to grant-extend, failure triggers that operator's spill. One accounting universe. |
| 29 | **NEW — Tantivy writer + merge budgets; mmap read path** | — | **NEW** | Part 2C. |
| 30 | **NEW — Owner working sets (stage 2)** | — | **NEW** | Bounded by construction: last committed value + uncommitted batch per owned key; `graydb.owner_working_set` cap 512MB; cold-key eviction back to router. |
| 31 | **NEW — Tail cache on log nodes** | — | **NEW** | Part 2D. |
| 32 | **NEW — The memory broker itself** | — | **NEW** | Part 3. |

Reading of the table: 14 subsystems inherited untouched, 5 modified with surgical diffs (each a named rebase cost), 4 deleted because their reason to exist is gone, 9 new. The inherit column is the compatibility promise made concrete; the new column is where all Phase-1 memory engineering lives.

---

## Part 2 — Per-node deep dives

### 2A. Compute node (reference: 64GB)

Memory map at steady state:

```
System        6GB   OS, allocator slack, SLRUs, lock tables, stats
shared_buffers 6GB  catalog + hottest pages (huge pages, locked)
LFC index    ~0.4GB RAM index over 400GB NVMe cache (100B × 4M chunks of 128KB... 
                    → chunk=1MB, 400K entries ≈ 40MB; 128KB chunks ≈ 320MB. LOCK: 1MB chunks)
Work         36GB   query grants (PG operators + any pushed-down DataFusion fragments)
Connections   4GB   ~400 pooled backends × 10MB floor + relcache caps
Append rings  0.5GB per-stream MPSC buffers + in-flight quorum window
Slack         ~11GB burst headroom inside cgroup memory.high
```

Query lifecycle with grant flow: parse/rewrite/plan allocate in normal per-query contexts (Connection quota; plans are KB–MB). At executor start, the estimated peak (from plan: Σ operator estimates × learned correction factor, Part 3.4) becomes `request_grant`. Execution debits the grant as contexts grow; operators that hit their share spill (sort → tapes, hash → batches — PG's existing spill code paths, unchanged). Parallel workers extend the same grant (item 11). Grant released at executor end; portal-held cursors retain a residual grant sized to their tuplestore.

LFC mechanics (locked design): chunk = 1MB of row-store pages; RAM holds only a hash index + clock bits (~40–80MB for 400GB); reads check shared_buffers → LFC → row-store service; writes (from log apply on read replicas) invalidate LFC chunks by LSN. LFC is *per-branch keyed* — branch_id is part of the chunk key, so branches never pollute each other's cache. Eviction: clock over chunks. Failure: LFC loss is free (it's a cache of a cache).

### 2B. Row-store node (reference: 128GB)

```
System        8GB
Page cache   92GB   O_DIRECT, huge pages; replacement = 2Q (A1in FIFO 25% / Am LRU 75%)
Undo buffers  4GB   ring of recent undo pages (inside cache share above: 92 = 88 pages + 4 undo)
Apply floor  12GB   inviolable: decode 2GB → route 1GB → per-partition apply 8GB → flusher 1GB
Work          6GB   compaction, snapshot cuts, integrity scans (grant-based, Maintenance class)
Slack        10GB
```

Replacement policy decision (LOCK): **2Q**, not clock-sweep, not ARC. Rationale: apply traffic and analytical replays generate massive one-touch scans; 2Q's probationary A1in queue absorbs them without evicting the resident hot set, at O(1) per access and no patent/complexity burden (ARC). Revisit trigger: hit-rate telemetry shows >5% regression vs a shadow-simulated LRU on production traces.

Apply pipeline (the Apply pool's anatomy): stage 1 decode (log records → typed rows; 2GB ring, backpressures the log tail if full); stage 2 route (hash by relation OID / partition → per-partition queues; 1GB); stage 3 apply workers (N = cores/2; each owns partitions exclusively — no page latch contention across workers; 8GB working memory for page modifications + index maintenance); stage 4 flusher (dirty-page writeback by LSN order for snapshot consistency; 1GB). **The floor is the sum of stage minimums; the broker may grow Apply during catch-up storms by shrinking Work, never the reverse.**

Undo accounting: undo pages live in the page cache like any page; the *binding* resource is retention: `undo_retention_target` (1h default) × undo generation rate = disk footprint, surfaced in `graydb.memory` as `undo_retained_bytes` + `oldest_snapshot_age`. RAM never bounds undo; the `snapshot too old` error bounds time.

### 2C. Columnar + search node (reference: 64GB)

```
System        6GB
Segment cache 24GB  parquet footers/column chunks (16GB, broker-tracked exactly)
                    + Tantivy mmap estimate (8GB, tracked approximately — see residual R1)
Work         22GB   DataFusion operators (16GB) + Tantivy IndexWriter (4GB) + merges (2GB)
Apply floor   8GB   columnar row-group builders (5GB) + search ingest queues (3GB)
Slack         4GB
```

DataFusion integration (the load-bearing mechanism): GrayDB implements DataFusion's `MemoryPool` trait backed by the broker. Every operator (`GroupedHashAggregateStream`, `SortExec`, `HashJoinExec`...) registers a `MemoryConsumer`; `try_grow(n)` → broker grant-extend; on denial DataFusion's own spill machinery activates (sort spills runs, hash agg spills partitions). Consequence: **PG operators and DataFusion operators compete in one accounting universe** — a PG-side hash join can force a DataFusion sort to spill and vice versa, by policy (Part 3.5 fairness), not by accident.

Tantivy budgets: each active `IndexWriter` gets `min(256MB, work_search_share / active_writers)`, floor 64MB (below which Tantivy segment-building thrashes); ingest queue backpressures the log tail when writers are budget-starved — converting memory pressure into bounded freshness lag, per the inviolable coupling. Merges are scheduled (max 1 concurrent large merge per index, 2GB cap) — merge storms are a known Lucene/Tantivy failure genre and are throttled by budget, not by hope.

Search read path: mmap'd segments. Accounting strategy (LOCK for Phase 1): cgroup-level page-cache attribution + periodic `smaps_rollup` sampling per process → reported as an *estimate* band in `graydb.memory`, and the segment-cache budget is set 20% conservative to absorb estimate error. Honest residual R1: exact per-query attribution of mmap pages is not achievable without a custom Directory implementation; deferred with trigger (Part 7).

### 2D. Log node (reference: 32GB)

```
System        4GB
Tail cache   20GB   recent log segments served to subscribers from RAM
Append path   2GB   per-stream rings + in-flight quorum window
Group buffers 2GB   batch assembly + checksum + replication staging
Slack         4GB
```

Append ring sizing (derivation, not vibes): in-flight bytes = target throughput × replication RTT × pipelining depth. 250MB/s × 2ms RTT × 4 in-flight batches ≈ 2MB per hot stream; 2GB supports ~1000 concurrently hot streams (stage-1 multi-tenant). Tail cache sizing: Σ over subscribers of (allowed lag × ingest rate); 20GB at 250MB/s aggregate covers 80s of aggregate subscriber lag before any catch-up read touches disk — chosen to make T3-style bursts RAM-served. `graydb.memory` exposes `tail_cache_hit_ratio`; sustained <0.95 is the "add tail RAM or add apply capacity" alert.

---

## Part 3 — The broker, specified

### 3.1 State
Per node: `pool[class] = {budget, reserved, granted}` with class ∈ {System, Cache, Work(Query, Maintenance), Apply, Connections}. Grants are a tree: node → class → (tenant) → query → operator-consumer. All counters are per-CPU-sharded atomics folded on read (accounting reads are frequent for observability; contention on the hot path must be ~zero).

### 3.2 API (internal)
```
request_grant(class, tenant, est_bytes, deadline) -> Grant | Queued | Denied
grant.extend(delta)  -> Ok | SpillAdvised | Denied     # SpillAdvised = soft limit crossed
grant.release(final_peak)                              # feeds the correction loop
broker.pressure(class) -> {none, soft, hard}           # consumed by admission + apply coupling
```
Reservations are chunked at 1MB granularity — a grant's live counter moves in 1MB steps to keep atomic traffic off the allocation fast path (allocations inside a chunk are context-local).

### 3.3 Admission & victim policy
Queue when Work granted > 85% (soft); deadline default 5s → `53200` to the waiter with queue diagnostics. Hard pressure (>97% node-wide or cgroup PSI-memory > threshold): victim = argmax over running queries of `granted_bytes × (1/priority) × (1 − progress_fraction)` — evict the biggest, lowest-priority, least-finished query first; progress from executor instrumentation. One victim at a time, re-evaluate after 250ms.

### 3.4 Estimation & correction loop
Initial estimate = plan-derived Σ per-operator estimates. Per (plan-shape fingerprint) the broker keeps EWMA of `actual_peak / estimate`; future grants are pre-scaled by it, clamped [0.5, 4.0]. Misestimation beyond clamp → `SpillAdvised` path rather than grant explosion. This loop is the answer to "PG's estimates are wrong" — we don't need them right, we need them *corrected and bounded*.

### 3.5 Fairness & tenancy
Work(Query) subdivides per tenant by weighted max-min share (weights = plan tier); a tenant may burst into unused share, is first reclaimed under pressure. Maintenance is preemptible by Query above 70% Work utilization. Apply preempts nothing and is preempted by nothing (the invariant).

### 3.6 Couplings (restated as spec)
Apply floor never shrinks. Apply saturation → `broker.pressure(Apply)=hard` → gateway write-admission slows (token bucket refill −50% per pressure tick) before freshness lag exceeds `bounded()` guarantees. Memory pressure is converted to bounded latency, never divergence — this line is testable and appears in the kill-gate addendum below.

---

## Part 4 — Parameter surface (GUC table, Phase-1 complete set)

| Parameter | Default | Range | Change | Notes |
|---|---|---|---|---|
| `graydb.work_pool` | 56% of node − fixed | 20–70% | restart | Work class size |
| `graydb.apply_floor` | role-profile | ≥ stage minimums | restart | inviolable |
| `graydb.maintenance_share` | 15% of Work | 5–40% | reload | preemptible |
| `graydb.grant_queue_timeout` | 5s | 100ms–5min | session | 53200 on expiry |
| `graydb.grant_soft_limit` | 85% | 50–95% | reload | queue threshold |
| `graydb.relcache_cap` | 32MB | 8MB–1GB | session | LRU eviction |
| `graydb.owner_working_set` | 512MB | 64MB–8GB | reload | stage 2 |
| `graydb.undo_retention_target` | 1h | 1min–7d | reload | snapshot-too-old horizon |
| `graydb.lfc_size` | 80% NVMe free | — | restart | compute cache |
| `graydb.lfc_chunk` | 1MB | 128KB–8MB | restart | locked 1MB |
| `graydb.tail_cache` | role-profile | — | restart | log nodes |
| `graydb.search_writer_budget` | auto (64–256MB) | — | reload | per active index |
| `graydb.merge_concurrency` | 1 large/index | 1–4 | reload | Tantivy merges |
| `work_mem` | 64MB | — | session | **hint only** |
| `hash_mem_multiplier` | 2.0 | — | session | estimation input |
| `jit` | off | — | session | surcharge if on |
| `huge_pages` | on | — | restart | fail loudly |
| inherited PG memory GUCs (`shared_buffers`, `temp_buffers`, `maintenance_work_mem`→mapped, SLRU sizes, `logical_decoding_work_mem`, `max_locks_per_transaction`, …) | PG defaults unless stated | — | — | semantics per Part 1 |

---

## Part 5 — Pathological scenarios (design-time adversarial table)

| Scenario | What happens, step by step | Bounded by |
|---|---|---|
| 10x grant misestimate on a giant hash join | extend → SpillAdvised at soft limit → PG hash spills batches → finishes slow; EWMA corrects future runs | grant + spill |
| 500 sessions each open 5 hash joins (the classic 62GB PG bomb) | Σ grants hits Work; queries 401+ queue; timeout 53200 with diagnostics | admission |
| One backend touches all 2,000 relations | relcache LRU evicts at 32MB; extra catalog re-reads, no growth | relcache_cap |
| Parallel query ×8 workers each ballooning | one shared grant; extend denied → planner-degraded parallelism | grant tree |
| Logical decoding of a 50GB transaction (sidecar out) | ReorderBuffer spills at 64MB per PG; Work-accounted | inherited spill |
| COPY of 1TB into heap | ring-buffer strategy caps cache footprint (inherited); append rings backpressure at log bandwidth | rings + strategy |
| Apply catch-up after 30min materializer outage | Apply grows into Work (never reverse); strong reads honest-block; write admission slows at hard pressure | coupling invariant |
| Tantivy merge storm on 40 indexes | scheduler: 1 large merge/index, global 2GB merge cap; ingest continues at writer floor | merge budget |
| Hot-key owner handed 1M keys by governor bug | owner_working_set cap → cold-key eviction to router; escalation rate-limited | cap + governor limits |
| Branch bomb: agent creates 5,000 branches | LFC keyed per branch — no cache blowup; branch metadata KB-scale; S3 cost is the only growth | keying + budget |
| cgroup co-tenant steals host RAM | memory.high PSI signal → broker treats as hard pressure (ladder step 3→4) before kernel reclaim stalls us | PSI hook |
| glibc/jemalloc fragmentation after 30-day churn | RSS − Σ accounted > 8% alert (`fragmentation_ratio` gauge); mitigation = arena tuning; soak test required | observability + R2 |

---

## Part 6 — Observability contract (schema)

`graydb.memory(node, role, pool, sub_pool, budget_bytes, reserved_bytes, granted_bytes, queued_grants, spills_per_s, evictions_per_s, apply_floor_ok, tail_cache_hit_ratio, undo_retained_bytes, oldest_snapshot_age, fragmentation_ratio, mmap_estimate_low, mmap_estimate_high)`.
`graydb.grants(query_id, tenant, class, estimate, granted, peak, spilled_bytes, state, wait_ms, victim_score)` — live grant table; the "who is eating the box" view.
Per-query: `peak_mem`, `spilled_bytes`, `grant_waits` in `pg_stat_statements`; `EXPLAIN (ANALYZE, MEMORY)` shows estimate vs grant vs actual per node. Backend introspection: `pg_backend_memory_contexts` inherited; cross-process context dump inherited where the PG version provides it.

---

## Part 7 — Honest residuals (named, with triggers)

R1 mmap attribution is an estimate band (smaps sampling); trigger to build a custom Tantivy Directory with exact accounting: estimate band width >15% of node RAM in practice. R2 allocator fragmentation across glibc(PG)+jemalloc(Rust) is untested; 30-day churn soak is a Phase-1 exit requirement. R3 grant-estimation cold start (no EWMA history) over-queues conservative workloads for the first hours; acceptable, documented. R4 process-per-connection floor inherited; revisit only if a future PG threading model lands upstream. R5 every budget number is designed; the adversarial-mix simulation (risk #8) and G-M gate below are the tests.

Kill-gate addendum (extends Chapter 18): **G-M** — under the adversarial mix (70% small OLTP grants + 3 concurrent giant analytics + full apply-storm catch-up, 2h sustained): zero kernel OOM events, p99 grant wait ≤ 2× queue timeout for priority class, apply floor intact throughout, strong-read staleness ≤ bounded() promises. Fail → the broker design reopens before any Phase-2 work.

---

## Part 8 — LOCK LIST (frozen for Phase 1; each with revisit trigger)

L1 Contexts inherited, block-level accounting hooks (trigger: hook overhead >0.5% CPU). L2 Dual buffer strategy: small shared_buffers + LFC on compute; 2Q page cache on row-store (trigger: 2Q hit-rate regression >5% vs shadow LRU). L3 LFC chunk = 1MB, per-branch keyed. L4 `work_mem`=hint, grants binding, 1MB reservation chunking. L5 Maintenance = preemptible 15% sub-class. L6 relcache/catcache LRU caps ON by default at 32MB (trigger: measurable catalog-thrash on <500-table schemas). L7 Append rings replace wal_buffers; BDP-derived sizing. L8 DataFusion MemoryPool = broker (single universe). L9 Tantivy writer floor 64MB / cap 256MB; 1 large merge per index. L10 mmap = estimate-band accounting for Phase 1 (R1 trigger). L11 Apply-floor invariant + write-admission coupling exactly as §3.6. L12 huge_pages=on, jit=off defaults. L13 Victim policy formula as §3.3. L14 EWMA correction clamp [0.5,4.0]. L15 Degradation ladder order: spill → evict → queue → cancel(53200) → never-OOM.

Self-rating of this spec: 8.5/10. Strong: complete PG inventory with dispositions (nothing waved at), derivations for the sizes that matter, one accounting universe across PG+DataFusion+Tantivy, and a falsifiable gate (G-M). Named weaknesses: R1 (mmap estimates), R2 (allocator soak untested), and the relcache-eviction patch is the single largest upstream-divergence in the whole compute layer — its rebase cost is real and recurring.
