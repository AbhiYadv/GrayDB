# GrayDB — Design Specification v0.3

Status: working draft under active adversarial review
Method: every load-bearing claim must survive a contradiction audit and, where possible, a quantitative trial. Claims that failed are recorded, not deleted.

---

## 1. Problem statement and market evidence

Developers keep choosing Postgres (55%+ adoption, still rising) and keep hitting the same five walls. Evidence gathered from primary sources (HN threads, migration post-mortems, vendor deprecation notices, review sites):

1. **Write scaling requires re-architecture.** Native HA/sharding is DIY (Patroni/Citus). Every attempt to fix it compromised something users refuse to lose: CockroachDB cost ~3x comparable Postgres for similar per-region performance (ZITADEL post-mortem) and broke the compatibility long tail; Aurora DSQL shipped without FKs, triggers, views, JSONB, extensions and was rejected as "barely a database"; Timescale multi-node reached ~1% adoption and was deleted by its own vendor.
2. **Vacuum / bloat / wraparound ops debt.** Two decades of identical complaints; the community's chosen fix direction is undo-log MVCC (OrioleDB), i.e., replace heap mechanics, don't tune them.
3. **Workload sprawl + sync pipelines.** Postgres + Elasticsearch + ClickHouse + a vector DB, each with a pipeline. The market's revealed preference is consolidation (pgvector's win over standalone vector DBs; ParadeDB for search; pg_duckdb for OLAP). Sync pipelines are the enemy.
4. **Agent-era economics.** >80% of Neon databases are created by AI agents; 97% of branches. Sub-second provisioning, copy-on-write branching, scale-to-zero are becoming table stakes ($1B Neon acquisition as market confirmation).
5. **Honesty as a feature.** DSQL's backlash shows that compatibility and pricing claims are audited by the community line-by-line. Overclaiming is a product defect.

Strategic synthesis: the market punishes distributed-by-default (cost/latency tax on the 95%) and punishes bolt-on distribution (Timescale multi-node, Citus friction). The winning shape: excellent single-node start, progressive distribution as an opt-in per table/tenant, never a migration.

## 2. Architecture v0.3 (post-trial)

Four layers. Truth lives in layer 2 + 4; everything else is disposable-with-caveats.

**L1 Compute — real Postgres, stateless.** Actual PG parser/planner/executor/extension API; storage assumptions removed (no local WAL, no checkpointing from compute). Compute nodes hold caches, not truth. Consequences: genuine extension compatibility on heap tables (see R1), instant spawn, scale-to-zero, branches as metadata. Cost: perpetual rebase on PG majors (accepted; Neon precedent).

**L2 Shared log — the one hard thing.** Replicated, ordered, row-level WAL service. Commit = quorum append (2 of 3). All consensus in the system lives here; compute and materializers run none. Single-writer PG semantics per database by default. Measured cost (Trial 1): single-connection commit p50 ~0.9ms vs ~0.3ms local NVMe — a 2–4x latency regression, traded for 1.5x better p99 tails (no checkpoint stalls), elasticity, and the materializer model. This trade is published, not hidden. Throughput ceiling per stream is bandwidth (~500K txn/s @ 0.5KB), not IOPS.

**L3 Materializers — one log, three read shapes.**
- *Row store:* dual-engine. Stock heap is the default (every extension and index AM works — GIN, GiST, SP-GiST, BRIN, PostGIS, pg_trgm). Undo-MVCC engine (`WITH (engine='undo')`) is opt-in for high-churn B-tree workloads where vacuum hurts most. GIN/GiST on the undo engine is a multi-year roadmap item, stated as such.
- *Columnar:* aged log segments compacted into compressed columnar files on object storage; executed via embedded DataFusion behind vectorized custom scan nodes with filter/agg pushdown (ADR-002). PG's executor handles joins/stitching above.
- *Search:* Tantivy BM25 + vector indexes fed from the log in commit order.
- Consistency is per-query, first-class SQL: `strong` (wait for applied-LSN ≥ snapshot-LSN), `bounded(X ms)` (error fast if staleness exceeds X), `eventual`. Same LSN-token mechanism provides read-your-writes on read replicas.

**L4 Object storage.** Cold segments, snapshots, branch history on S3. Branch = metadata fork (copy-on-write). Recovery = snapshot + delta replay (pure replay rejected by Trial 4 RTO math). PITR, branching, and replica creation are the same primitive.

**Progressive scaling.**
- Stage 0 (default): one writer, elastic readers, full PG semantics, no consensus in read path.
- Stage 1 (opt-in): log stream per tenant; write scale-out with zero cross-tenant coordination.
- Stage 2 (opt-in, redesigned by Trial R3): **owner-first execution**. Hot keys are assigned owners that serialize writes in memory (~33K writes/s/key ceiling) and group-append to the log; the cold tail runs OCC as a fast path. A contention governor escalates keys on abort-rate spikes with <100ms handover. Cross-owner multi-key transactions use ordered owner acquisition (deadlock-free by key order) — the protocol is the top remaining design risk. Workloads with extreme single-key contention (>33K writes/s/key) require commutative batching or key-splitting; documented as a physical limit.

**Explicit non-goals.** Global active-active multi-region writes; MySQL compatibility; proprietary query language; building our own optimizer or vectorized executor.

## 3. Contradiction audit (v0.1 claims that failed)

- C1: "Transactionally consistent search" contradicted "async materializer." Resolved via consistency classes; strong reads pay replay lag (Trial 3 quantifies).
- C2: "Extensions work day one" contradicted undo-MVCC engine (index AMs are heap-TID-coupled; OrioleDB precedent). Resolved via dual-engine tables, heap default.
- C3: "Solves write scaling" was half-true: log offload removes I/O, not the single-writer CPU execution ceiling. Parallel write apply is a separate, explicitly-scoped project.
- C4: Commit latency regression (2–4x single-connection p50) was unstated. Now a published spec line.
- C5: Stage-2 OCC reintroduced DSQL-style retry loops. Confirmed catastrophically by Trial 2; redesigned owner-first (Trial R3).
- C6: "No backups, just replay" fails RTO math at scale (Trial 4). Snapshot + delta replay retained; replay remains the unifying primitive, not the sole recovery story.
- C7: "Planner stitches row+columnar transparently" hid the Volcano tuple-at-a-time executor problem. Resolved by embedding DataFusion (ADR-002) rather than building an executor.

## 4. Trials — methods and results

**T1 Commit latency (Monte Carlo, 200K samples).** Assumptions: local fsync lognormal median 0.30ms with 0.2% stall probability; cross-AZ one-way 0.35ms; quorum 2-of-3. Result: local p50/p95/p99 = 0.30/0.64/0.89ms; GrayDB 0.93/1.18/1.30ms. p50 regression 3.1x; p99 ratio 1.46x in GrayDB's favor directionally on tails (stall masking). Sensitivity: 2–4x p50 depending on AZ topology.

**T2 OCC abort vs contention (Monte Carlo + analytic).** Model: T tps, 4 writes/txn, 5ms window, 1M keys. Uniform access: 0.004% aborts @500tps, 0.4% @50K tps — OCC free. Zipf a=0.8 (top key 1.3% of traffic): 23% aborts @20K tps. a=1.01: 79%. a=1.2: 98%. Finding: OCC cliffs under skew; it does not degrade gracefully.

**T3 Materializer replay lag (queueing sim).** 10x ingest burst (40→400 MB/s, 120s) vs apply capacity: at 200 MB/s apply, staleness peaks at 120s and strong-consistency reads block for ~4.5 minutes total. Indexing costs 3–10x raw append. Consequence: consistency classes are mandatory, plus elastic burst capacity on materializers.

**T4 Rebuild RTO (analytic).** Pure log replay: 1TB = 43min (1 node) / 5.3min (8 nodes); 10TB = 7.1h / 53min. Snapshot + 5%-churn delta replay: minutes at any size. Consequence: snapshots stay.

**T5 Engine bake-off: DuckDB 1.5.5 vs DataFusion 54 (8M-row parquet, 1 core, identical SQL).** Warm medians: full-scan agg 0.14s vs 0.37s (DuckDB 2.7x); join+agg 2.1x DuckDB; time-pruned agg 0.45x (DataFusion faster); selective needle 14x DuckDB. Key finding: with MVCC visibility predicates (every GrayDB query), the gap collapses to 1.26x (0.32s vs 0.40s) — DuckDB's visibility-filter overhead 2.29x vs DataFusion's 1.07x. Limitations: 1 core, page-cached data, Python binding overhead, small reps.

**T6 (R3) Contention governor.** (a) Ceilings: OCC per-key ~30–50 writes/s; owner-serialized in-memory RMW stable to ~33K writes/s/key (~1000x) at ~1ms p50. (b) Steady state @20K tps: escalating 144–280 keys (≤0.03% of keyspace) cuts aborts 23→2.6%, 79→4.7%, 98→6.5%; but those keys carry 45–75% of traffic, so 90–99.6% of txns take the owner path → design inverted to owner-first with OCC cold tail. (c) Flash-sale transient (0→3K writes/s step): ~780–900 aborts during ~100ms escalation; retry storm peaks 5x offered load. Levers: abort-spike triggering, <100ms handover, client backoff.

## 5. Architecture decision records

**ADR-001 — Skeleton: shared-log disaggregation with real PG compute.** Options: fork-PG-upward (chosen), distributed-first with PG faked (rejected: compatibility long tail, cost/latency tax — CRDB/DSQL evidence), stock-PG extension (rejected: Timescale multi-node/TAM post-mortems show the coordination machinery fights bolt-ons). Consequence: perpetual rebase cost accepted.

**ADR-002 — Columnar execution: embed DataFusion.** "Build" rejected (two mature engines within ~3x; negative-sum race). DuckDB rejected on fit despite raw-speed win: measured gap on visibility-filtered (GrayDB-shaped) queries is ~1.26x, below the threshold to override Rust-native TableProvider integration (scan-level MVCC injection), in-process memory/scheduling control, Apache governance, and direct architectural precedent (InfluxDB 3.0, GreptimeDB). Revisit trigger: >3x gap on visibility-filtered TPC-H-derived suite at 8+ cores, or MVCC pushdown overhead >1.3x.

**ADR-003 — Stage-2 concurrency: owner-first deterministic execution, OCC cold tail.** Forced by T2 (OCC cliff) + T6 (owner path dominates under real skew). Open protocol work: cross-owner multi-key transactions via ordered owner acquisition.

## 6. Risk register (live)

| # | Risk | Severity | Status / mitigation |
|---|------|----------|---------------------|
| 1 | Cross-owner transaction protocol (multi-hot-key txns) | High | Undesigned; next trial target |
| 2 | PG rebase burden per major version | High | Accepted cost; Neon-scale team precedent; budget it |
| 3 | DataFusion perf gap on wide scans | Medium | Bounded 1.26–2.7x per core; revisit trigger set |
| 4 | Undo-engine space amplification under churn | Medium | Phase-1 gate G4 |
| 5 | Materializer freshness under sustained (not burst) overload | Medium | Elastic capacity + bounded-staleness class; needs sizing model |
| 6 | Commit-latency regression harms chatty-ORM workloads | Medium | Publish spec; pipelining + batching guidance; local-region topology option |
| 7 | Single-log-stream bandwidth ceiling for mega-tenants | Low | Stage 1/2 sharding path exists |

## 7. Phase-1 kill gates (falsifiable)

- G1: TPC-C-like OLTP on dual-engine ≥0.9x vanilla PG throughput, p99 ≤1.5x — else the storage rewrite isn't paying.
- G2: Hybrid row+columnar ≥10x vanilla PG on cold aggregates with <10% pure-OLTP regression — else HTAP claim dies.
- G3: Strong-consistency search p99 ≤150ms at 2x provisioned burst — else consistency classes are theater.
- G4: Undo engine survives 72h @50% update churn with <5% space amplification — else bloat was rebuilt with extra steps.

## 8. Build phasing

- Phase 1 (single-node wedge): PG compute + log-structured storage + undo engine (opt-in) + DataFusion columnar + Tantivy search. Sellable alone: "Postgres without vacuum, with built-in analytics and search."
- Phase 2 (disaggregation): log service, S3 tiering, branching, scale-to-zero, elastic readers, consistency classes.
- Phase 3 (progressive distribution): per-tenant streams; owner-first per-table sharding with contention governor.

Each phase independently shippable; the graveyard is full of designs that needed Phase 3 to sell anything.

## 9. Open questions, ranked

1. Cross-owner transaction protocol: latency and abort model for ordered acquisition (next trial).
2. Parallel write apply on the single-writer path (addresses C3 fully).
3. Materializer capacity sizing model (steady overload, not burst).
4. Rebase strategy: patch-set size budget and upstream-contribution policy.
5. Pricing model that survives DSQL-style community audit (predictability > cleverness).
