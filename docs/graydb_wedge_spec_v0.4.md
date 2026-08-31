# GrayDB Wedge — Deep Specification & Lock (v0.4)

## 0. System identity (architect's ruling)
GrayDB **is a database**: a derived, log-structured, multi-model DBMS for PostgreSQL whose write API is the Postgres logical replication protocol. Anatomy check against the canonical DBMS decomposition: client manager, query processor, executor (PG + DataFusion), access methods (columnar segments + Tantivy + LSN visibility), buffer/memory manager (broker), log manager & recovery (the LSN log + snapshot/delta replay), catalog (LSN-versioned registry), replication service (ingest = the write path) — all present. Deliberately absent: user write transaction manager, vacuum, checkpointer, backup-as-truth (removals by design; the source PG supplies serialized writes). Adjacent architectural precedents (patterns, not validation of this exact architecture): Elasticsearch proves the derived-read-model category monetizes at scale (though it accepts direct writes; the pipeline is its common deployment, not its write path); TiFlash proves columnar-as-log-subscriber inside TiDB's Raft/MVCC machinery; CDC-fed warehouses prove the demand with weaker consistency. The exact shape — an externally derived multi-model database driven by PostgreSQL logical replication with provable per-query LSN freshness — is unbuilt; that is the whitespace claim, held as hypothesis until W-gates convert it.
Category language (fixed): engineers — "derived database for Postgres"; buyers — "the search and analytics database for your Postgres"; never — "sync tool", "read-only layer" (external), "streaming platform".
**Constitution (every feature must pass all five):** I1 source of truth is external and singular — no user writes in v1. I2 one durable LSN-ordered log is the spine; every shape derivable from it. I3 shapes are disposable; snapshot+replay reconstructs anything. I4 every query answers under a declared consistency class and can prove its LSN. I5 the customer's database is sacred: SQL-objects-only footprint, bounded WAL retention, capped read impact.
Build order (dependency-true): M1 ingest/slot → M2 durable log → M3 schema registry/DDL pack → M4 correctness-harness skeleton (before the shapes — the W3 machine watches from the first byte) → M5 columnar → M6 search → M7 reader-head extension (S1 spike parallelizable) → M8 control mini.

Purpose: close the named gaps that held design ratings below lock quality (the "missing 2.5"), specify the product to component depth, and freeze the wedge behind a lock list with revisit triggers. Companion to RFC-0004 (which states *what and why*; this states *how, exactly*).

---

## 1. Product definition, one paragraph, no romance
GrayDB attaches to an existing PostgreSQL primary (self-hosted or managed: RDS, Aurora, Cloud SQL) via native logical replication, lands every change in its own durable LSN-ordered log, and materializes two read shapes — compressed columnar segments on object storage (queried through embedded DataFusion) and search indexes (Tantivy BM25 + HNSW vectors) — served through a GrayDB-operated stock-PostgreSQL reader head with a `graydb` extension. Every query runs at a declared consistency class (`strong | bounded(X) | eventual`) and can report the exact source LSN it reflects. The system is read-only and derived: the customer's PG remains the sole source of truth; every GrayDB store is rebuildable from snapshot + log replay.

**The correctness invariant (the product, formally):**
For every LSN L acknowledged by GrayDB: `Materialized(table, L) ≡ SourceSnapshot(table, L)` — **semantically equivalent row multisets** under (a) the Type Interpretation Contract (type, collation, timezone and encoding rendering rules, stated per supported type), (b) the schema version in force at L, and (c) the Table Eligibility Contract (Amendment A) — across crashes, failovers, rebuilds, and DDL. Byte-identity is explicitly not the claim; logical replication delivers values, not pages. Gate W3's harness is a property-based machine that generates randomized workload + DDL + fault schedules and checks this invariant at sampled LSNs. The harness is a first-class component, not a test folder.

## 2. Component map (with build-size honesty)

| Component | Responsibility | Tech | Est. size | Risk |
|---|---|---|---|---|
| Source attach pack | Publication mgmt, slot lifecycle, event-trigger DDL pack (plain SQL), health checks, WAL-retention guard (W5) | SQL + Rust ctl | 6–10 kLOC | Low — native PG features only |
| Ingest + durable log | pgoutput decode → LSN-ordered log records → local NVMe + S3 segments; **never lags** (WL5) | Rust | 8–12 kLOC | Low-med |
| Schema registry | LSN-versioned schema history; consumes in-stream ddl_log + Relation messages; drives typed re-materialization | Rust | 5–8 kLOC | Med — the moat |
| Columnar materializer | Log → parquet-class segments + delete bitmaps; compaction; per-table priority apply | Rust + DataFusion | 10–15 kLOC | Med |
| Search materializer | Log → Tantivy segments (BM25 + HNSW), commit-order, merge-throttled | Rust + Tantivy | 8–12 kLOC | Med |
| Reader head | Stock PG + `graydb` extension: custom scans w/ LSN-visibility pushdown, `@@@` operator, consistency GUCs, freshness views | C + Rust (pgrx) | 8–12 kLOC | **Highest — gated spike (§3)** |
| Control mini | Freshness telemetry, apply autoscaler (T7 policy), initial-load orchestrator | Rust | ~5 kLOC | Low |
| Correctness harness (W3 machine) | Property-based invariant checker; fault + DDL schedule generator | Rust | 10 kLOC+ | The long pole, on purpose |

Total ≈ 60–85 kLOC; consistent with RFC-0004's 5–8 engineers / 9–12 months to pilot grade. Solo, this confirms K2: build only after W1.

## 3. Gap A closed (design) — the extension-surface map

Decisive simplification discovered under analysis: **derived data on the reader head does not use PostgreSQL MVCC at all.** Visibility is an LSN predicate evaluated inside our scans (insert_lsn ≤ target ∧ not deleted-by ≤ target — exactly the shape trial T5 measured at 1.26x). PG's own snapshot machinery governs only catalog metadata. This removes the scariest imagined requirement (teaching PG snapshots about foreign LSNs) entirely.

| Wedge requirement | Stock-PG mechanism | Precedent | Residual |
|---|---|---|---|
| Columnar scan w/ pushdown | CustomScan API + `set_rel_pathlist_hook` | pg_duckdb, Citus columnar, ParadeDB | none |
| `@@@` search predicate | Operator + support functions + custom scan | ParadeDB (shipping) | none |
| `strong` wait / `bounded` error | ExecutorStart-hook: wait/compare applied_lsn vs target | trivial hook use | none |
| Consistency GUCs, freshness views | DefineCustomVariable, plain views over shared state | ubiquitous | none |
| Planner row estimates for segments | `get_relation_info`-level hooks fed from segment metadata | pg_duckdb pattern | estimate quality — tune during build |
| Background applied-LSN publisher | Background worker API | ubiquitous | none |
| Source-side needs | **Nothing installed**: publication + plain-SQL event triggers (RDS/Aurora/Cloud SQL compatible) | Debezium-era patterns | provider-policy drift → WL2 fallback |
| Escape hatch | Reader head is **our** deployable; a small patch set is acceptable if an API falls short — customer never runs patched PG | Neon-style ops | none in customer's world |

Verdict: no core patches expected; risk downgraded from "premise-threatening" to "confirm-in-code." **Gate S1 (2-week spike):** thin end-to-end scan at a target LSN through the extension path on stock PG 16/17. This is the honest unclosable-in-chat remainder.

## 4. Gap B closed — DDL architecture + the 20-pattern matrix

**Architecture (three layers, in trust order):**
1. **In-stream capture (primary):** event triggers (`ddl_command_end`, `sql_drop`, `table_rewrite`) write normalized command records into `graydb.ddl_log`; that table is in the publication → DDL arrives LSN-ordered, interleaved exactly with data, transactional with the DDL itself. The ordering problem that kills pipeline deployments does not exist in this design.
2. **Protocol cross-check:** pgoutput Relation messages (schema metadata sent when a published table's shape changes) verified against expected registry state; divergence ⇒ alert + reconcile.
3. **Reconciler (safety net):** periodic catalog snapshot diff catches anything event triggers can't see (restores, promotions, out-of-band surgery); repairs registry, triggers re-materialization where required.

**The matrix (W3's 20 patterns, each with its handling class):**
A=metadata-only, B=typed re-encode going forward, C=segment rewrite/backfill, D=policy decision.

| # | Pattern | Class | Note |
|---|---|---|---|
| 1 | ADD COLUMN (null/constant default) | A | registry version bump |
| 2 | ADD COLUMN volatile default (table rewrite) | C | table_rewrite event → resnapshot table |
| 3 | DROP COLUMN | A | tombstone in registry; segments lazily compacted |
| 4 | RENAME COLUMN | A | identity by attnum, not name |
| 5 | ALTER TYPE, binary-coercible | B | re-encode forward |
| 6 | ALTER TYPE w/ rewrite | C | resnapshot table |
| 7 | SET / DROP NOT NULL | A | constraint metadata |
| 8 | ADD / DROP primary key | B | delete-bitmap keying update |
| 9 | CREATE TABLE | D | auto-add-to-publication policy (default: opt-in list, `ALL TABLES` optional) |
| 10 | DROP TABLE | A | retire shapes; retain per retention policy |
| 11 | RENAME TABLE | A | OID-stable |
| 12 | TRUNCATE | A | pgoutput truncate message → shape truncate at LSN |
| 13 | Partition ATTACH / DETACH | B | routing map update |
| 14 | New partition child under published parent | D | follows #9 policy via `publish_via_partition_root` decision |
| 15 | ALTER ... SET SCHEMA | A | qualified-name registry |
| 16 | ENUM ADD VALUE | B | dictionary extension |
| 17 | Composite/domain type change | C | conservative: resnapshot dependents |
| 18 | GENERATED column add | B | value arrives in stream; no compute needed |
| 19 | IDENTITY/sequence changes | A | ignored (values arrive materialized) |
| 20 | Rewrite ops (CLUSTER, VACUUM FULL) | A | relfilenode change invisible at logical layer; verify via cross-check |

Classes C are the expensive ones; the design makes them *correct by resnapshot* first, *cheap* later. W3 requires all 20 green in the harness.

## 5. Gap C closed — initial load for multi-TB sources

**Mechanism (exact-LSN by construction):** create the logical slot with an exported snapshot → the slot's consistent point is LSN₀ and the snapshot name lets N parallel workers `SET TRANSACTION SNAPSHOT` and COPY disjoint ctid ranges of each table — every byte of the bulk load is the database *exactly at LSN₀*. Concurrently (not after), the ingest service consumes the slot from LSN₀ into GrayDB's log — the source's WAL retention stays bounded (W5) no matter how long the bulk load takes, because the slot advances while COPY runs; records are buffered cheaply in our log and applied per-table once that table's snapshot lands. Handoff invariant per table: `COPY(snapshot@LSN₀) + apply(LSN₀ → now) ≡ exact`, which is the §1 invariant's base case.

**Math and source-safety caps:** default read budget 25–30% of source I/O headroom, operator-tunable. Example, 5 TB source, 8 parallel streams at a capped ~80 MB/s aggregate-per-stream mix ⇒ ~640 MB/s raw ⇒ ~2.2 h uncapped, 4–8 h under polite caps — during which slot lag stays ~zero because ingest runs concurrently. Failure mid-load: per-table restartable (ctid ranges are idempotent units); no global restart.

## 6. Gap D closed — sustained overload (trial T7) + the decoupling law

**Law (WL5):** ingest never lags (protects the source; cheap append); apply may lag (freshness classes absorb). There is no backpressure to the source — by definition of the product.
**T7 numbers (sim, stated assumptions: 2x sustained surge 40 min; base apply 200 MB/s; autoscale +200 MB/s per step on staleness>30 s; 120 s provisioning):** no autoscale → peak staleness 2,400 s, promises broken ~95 min. Autoscale → peak 150 s; `bounded(30s)` violated ~4.5 min at onset only; scale-down clean.
**Published SLO fine print:** `bounded(300s)` survives any surge we can provision for; `bounded(30s)` carries a documented onset window ≈ trigger + provisioning + catch-up; sustained ingest above max provisioned capacity is a physics limit, alarmed as capacity exhaustion. **Priority tiers:** per-table apply priority so designated critical tables stay fresh first during catch-up.

## 7. Gap E locked (as hypothesis) — license + pricing
License: Apache-2 for the entire data plane (attach pack, ingest/log, materializers, extension, harness); closed: multi-tenant control plane, autoscaler policy engine, cloud console. Decided before any public code (V2 satisfied).
Pricing v1 hypothesis (discovery-tested, not code-locked): per connected source database (base) + materialized GB-month + apply-compute tier. Explicitly rejected: per-query/DPU-style metering (the DSQL pricing backlash is on file).

## 8. WEDGE LOCK LIST (WL1–WL12; each with revisit trigger)
WL1 Read-only derived layer; customer PG is sole truth (revisit: never in v1).
WL2 Zero installs on source: publication + plain-SQL event-trigger pack (trigger: a major managed provider blocking event triggers → catalog-diff-only degraded mode ships).
WL3 DDL = in-stream replicated ddl_log + Relation-message cross-check + reconciler; matrix of 20 with classes A–D (trigger: any pattern unimplementable in-stream → falls to reconciler class, documented).
WL4 Reader head = stock PG + extension; LSN-predicate visibility bypasses PG MVCC for derived data (gate S1 spike confirms; escape hatch = patches on our head only).
WL5 (amended v0.4.1) Decoupling law: **apply lag never blocks ingest while ingest capacity remains available**; there is no apply-side backpressure to the source. Ingest itself CAN stall (network, disk, decoder crash, oversized transactions, object-storage outage, source failover) — therefore source WAL retention is bounded by a hard budget (default min(50 GB, 4 h), configurable) with the limit ladder of Amendment A §A3, terminating in deliberate slot drop + continuity-loss event + WL7 resnapshot. I5's "bounded WAL retention" is defined by this budget and ladder, not by aspiration.
WL6 Overload = lag-triggered apply autoscale + published SLO fine print + per-table priority tiers (T7 evidence).
WL7 Initial load = exported-snapshot + parallel ctid-range COPY + concurrent slot ingest; per-table restartable.
WL8 Correctness harness is a component; W3 = all 20 DDL patterns + crash/failover/rebuild schedules green on the §1 invariant.
WL9 Engines: DataFusion (ADR-002 carried; T5 evidence) + Tantivy.
WL10 Consistency classes + `graydb.freshness` exactly as designed; `strong` = wait, `bounded` = fast error, `eventual` = never blocks.
WL11 Apache-2 data plane / closed control plane, decided pre-publication.
WL12 Pricing hypothesis per §7; the only WL item the market may rewrite.

## 9. Gates, not gaps — the honest remainder
S1 extension spike (2 weeks, code); W1 demand (5 pilots); W2 pipeline deletion in 90 days; W3 harness green; W4 retention + search p99 ≤150 ms @2x; W5 WAL-retention guard proven against a real managed source; G-M memory sim (single role). Nothing else about the wedge is undesigned.

## 10. Rating
9.5/10 at design completeness: every previously named weakness is either closed above (B, C, D, E, and A-as-design) or converted into a falsifiable gate that *cannot* be closed without code or market contact (S1, W1–W5, G-M) — and pretending otherwise would be the eleventh gap. The remaining 0.5 is reserved, permanently, for the difference between a locked design and a system that has met reality; no document closes it.

---

## Amendment A — v0.4.1 (post external review R2)

Provenance: second external GPT review, adjudicated in session. Six corrections accepted (one aggravated by our own verification), one pushback sustained (locks are versioned instruments; amend-and-relock, not unlock). This amendment supersedes §10's rating.

### A1. Materialize correction and competitive upgrade
"Materialize died" was false and is retracted. Verified current state: actively shipping (Self-Managed v26 line; v26.0.0 added upstream schema-change handling for PostgreSQL sources, swap-by-default, SASL/SCRAM), and repositioned as "the live data layer for apps and AI agents" — i.e., converging on our Act-2 narrative from the streaming-SQL side. Status upgraded: **named active competitor** on consistency story and agent story. Honest differentiation vs Materialize: they compute incremental views (general IVM, always-on dataflow economics, their own engine behind a PG-compatible surface); we materialize two fixed shapes (search + columnar) queried through stock PG with per-query LSN proof and rebuild-from-log economics. Overlap is real at "fresh derived data"; divergence is search, PG-native surface, disposability, and cost model. Track A discovery must include Materialize win/loss questions.

### A2. Precedent posture (correction 2)
ES/TiFlash/CDC-warehouses reclassified as adjacent patterns (see amended §0). The exact architecture is unbuilt — whitespace held as hypothesis, converted only by W-gates.

### A3. WAL-limit ladder (correction 3; completes WL5′ and I5)
Budget: `wal_budget = min(bytes_cap, time_cap)`, default min(50 GB, 4 h), per source, operator-set at attach.
Ladder: (1) ≥50% budget → warn + page; (2) shed nonessential ingest-side work (pause backfills, dedicate decode capacity to slot drain); (3) log write path degraded → spill decoded stream to local staging NVMe; (4) staging at risk or decoder unhealthy while log/S3 reachable → emergency durable raw append under the **hardened ack invariant**: `confirmed_flush_lsn` advances to L only when every frame from prior ack → L is durably in object storage in order (sequenced + checksummed), L is a transaction-complete boundary (commit / stream-commit; never mid-transaction), and the durable prefix is self-describing — guaranteed structurally because raw-capture mode NEVER splices a dying session's tail: it opens a fresh replication session from the last durably-acked LSN, forcing PostgreSQL to re-emit Relation/Type metadata for the new stream. Honest scope: rung 4 survives decoder/apply failures, not arbitrary mid-session capture. Replay = deterministic decode of the durable frame stream (S1 demo: induced decoder death → replay from frames alone, zero gap/dup); (5) budget exhausted or source unreachable-then-recovered-beyond-budget → deliberately drop the slot, emit continuity-loss event (SEV, customer-visible), execute WL7 resnapshot. Every rung is observable (`graydb.source_safety`: budget, consumed, rung, ETA-to-limit). Rung 4 is the key invention: slot advancement is decoupled from decode health, so only total connectivity loss reaches rung 5.

### A4. Consistency contract v2 (correction 4; supersedes the WL10 class definitions)
- `eventual` — never blocks.
- `bounded(X)` — staleness measured against a source heartbeat (`pg_current_wal_lsn()` sampled ~1 s); honest to the SOURCE within heartbeat resolution; fast error on violation.
- `read_your_writes(token)` — application supplies its post-commit LSN (`SELECT pg_current_wal_lsn()` after commit, or SDK helper); GrayDB waits until required shapes ≥ token. Recommended default for app RYW; requires app/SDK cooperation.
- `strong` — source-barrier semantics: at query start GrayDB obtains a barrier LSN from the source (one lightweight read, permitted under I5's read-impact cap), waits all required shapes ≥ barrier, executes. Cost disclosed: +source RTT + apply wait. Prior behavior ("strong relative to latest received LSN") is renamed internally `fresh_received` and is NOT exposed as `strong`.
Session default: `bounded(5s)`. WL10 amended accordingly.

### A5. Table Eligibility Contract (correction 5; referenced by the §1 invariant)
1. Identity: PRIMARY KEY (replica identity DEFAULT) or REPLICA IDENTITY INDEX supported; REPLICA IDENTITY FULL supported with documented WAL-inflation warning; REPLICA IDENTITY NOTHING ⇒ append-only eligibility (updates/deletes reject the table into `ineligible` state, surfaced at attach and on DDL).
2. Types: v1 supported list = core scalars, text under deterministic collations, numeric/decimal, timestamp/timestamptz (UTC-normalized rendering), uuid, bytea, jsonb, arrays of the above; enums via dictionary; everything else → per-type mapping table or `ineligible`, decided at attach, never silently.
3. TOAST: unchanged TOASTed columns absent from update images (unless RI FULL) are reconstructed by joining the prior materialized version at L−1 — a structural advantage of holding the previous state; rule is part of the invariant's interpretation contract.
4. Generated columns: consumed when the source publishes them (version-dependent); otherwise columns marked `not_materialized` — never recomputed by GrayDB in v1.
5. Oversized transactions: streaming in-progress decode enabled where protocol supports; hard cap with spill-to-staging; cap breach = documented behavior, not surprise.
6. DDL: per the 20-pattern matrix classes A–D (§4).

### A6. Log retention & durability model (correction 6; operationalizes I2/I3)
Algebra: `rebuildable(t) = validated_base_snapshot(B) + log[B → head]`.
- Base cadence: per table, triggered by log-since-base > 0.5× table size OR age > 7 d; new base validated by the harness invariant BEFORE the prior base and its exclusive log interval are retired.
- Historical-LSN queries: bounded by a configurable retention window (default 7 d); beyond-window requests error explicitly.
- Durability rule (the sentence that makes I3 true): **the source slot is acknowledged only up to the point durably persisted in object storage (multi-AZ)**. Local NVMe is staging, never truth. Consequence: losing every shape AND its required log interval requires regional object-storage loss; the declared recovery for that event class is continuity-loss + resnapshot (and optional cross-region log replication is the paid mitigation).
- Customer PG is re-read ONLY on declared continuity loss — never as routine recovery.

### A7. Sequencing ruling (adopted)
Pre-seed memo demoted: written only after (first pilot commitment) OR (S1 end-to-end green). Two tracks now:
- Track A (market proof): 10 named companies running PG→Debezium/Kafka→ES/CH; funnel gates: 5 acknowledge the problem, 3 share architecture/workload data, 2 agree to technical pilot, ≥1 has a budget owner. W1 (≥3–5 LOIs) remains the kill gate this funnel feeds.
- Track B (S1 correctness spike), acceptance = all eight demonstrations: exported-snapshot initial load; concurrent change ingestion; update+delete via replica identity; crash after log persistence but before materialization; replay without duplication or loss; caller-supplied target-LSN query; one additive + one destructive DDL; bounded source-WAL behavior under induced ingest stall (A3 ladder rungs 1–4 exercised).

### A8. Rating (three-axis, permanent format) and calibration rule
Design: 8.5/10 (four constitutional holes from R2 fixed on paper this session; fixes are fresh and unreviewed). Market proof: 0/10. Production proof: 0/10.
Calibration rule adopted: self-ratings are capped at 8.5 until an external review or reality contact has passed over the artifact; two consecutive external reviews finding real holes after 9+ self-scores is a measured bias, now corrected structurally.
Lock status: v0.4.1 — amended items re-locked; all other WL items unchanged.
