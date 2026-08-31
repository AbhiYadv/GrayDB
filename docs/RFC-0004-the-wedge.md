# RFC-0004 — The Wedge: GrayDB as a PostgreSQL-native search + analytics layer

Status: Accepted (ratified in design session, v0.4 baseline)
Supersedes: the v0.3 full-database scope as *product*; v0.3 remains the contingent platform hypothesis.

## Summary
GrayDB v1 is a read-only derived-data system that attaches to an existing PostgreSQL primary via logical replication, lands changes in its own durable logical log, and materializes two read shapes — columnar analytics (embedded DataFusion over object-storage segments) and search (Tantivy BM25 + vector) — queryable through a vanilla PostgreSQL head, with per-query, provable LSN freshness (`strong | bounded(X) | eventual`, `graydb.freshness`). Customer outcome: `Postgres → GrayDB → search + analytics` replacing `Postgres → Debezium → Kafka → {Elasticsearch, ClickHouse}`. The killer property is not "one database for everything"; it is: no sync pipeline, no stale-data mystery, and every result can prove exactly which source LSN it reflects.

## Motivation
1. External review (GPT memo, quoted in Alternatives) independently converged on our own Chapter-13 sidecar mode + Chapter-5 consistency classes as the product, and correctly indicted v0.3 scope as "five companies at once."
2. Self-audit admissions recorded: (a) our kill gates G1–G4/G-M contained zero demand gates; (b) our "Phase 1" was itself three companies; (c) we had mislabeled the product (sidecar) as merely the adoption path.
3. Market timing: the wedge only became a one-company build recently — PG failover slots (PG17) closed CDC's reliability tail; DataFusion/Arrow and Tantivy turned two engine-companies into libraries; PeerDB ($3.6M seed, late 2023 → ClickHouse acquisition, July 2024; fastest-growing CDC target connector) proves both demand and exit appetite for one quarter of this wedge.

## Detailed design (delta from v0.3)
- Source of truth: the customer's PostgreSQL. GrayDB is derived and rebuildable; disaster recovery is re-snapshot + replay, never restore-the-truth.
- Ingest: pgoutput logical replication → GrayDB durable log (single-writer, S3-backed; quorum service deferred). Slot-safety subsystem: ingest acks the source slot aggressively; GrayDB's own log is the retention buffer; hard cap on source WAL retention with automatic shed (gate W5).
- Query head: stock PostgreSQL + extension (custom scan nodes for DataFusion segments with MVCC visibility pushdown, `@@@` search operator, freshness/consistency GUCs and views). No fork. Rebase burden collapses to extension maintenance.
- Materializers, consistency classes, snapshot+delta rebuild, apply-pipeline anatomy, memory broker (single node-role) carry over from v0.3 unchanged.
- Schema evolution: DDL is not carried by logical decoding; GrayDB maintains LSN-versioned schema history via event triggers/DDL capture + typed re-materialization. This is the moat workstream (gate W3 includes a 20-pattern DDL matrix).

## Cross-layer impact
Deleted from v1: PG fork, gateway, quorum log service, undo engine and dual-engine tables, owner-first distribution + governor, branching/scale-to-zero, multi-tenant stage machinery. Shelved with evidence retained (T1, T2, T6 remain valid trials for the contingent platform). Retained and promoted: T3 (freshness-lag engineering is now the product), T4 (rebuild math), T5 (visibility-filtered DataFusion finding is core), memory spec Parts 2C/3/5–8 (single role), G-M gate.

## Kill gates (W-series; replaces G1/G4; G3 and G-M survive)
- W1 Demand: ≥5 companies with live PG→ES/CH pipelines commit to pilots.
- W2 Operational reduction: a pilot decommissions Kafka + Debezium + custom reconciliation within 90 days — removed, not wrapped.
- W3 Correctness: crash/failover/rebuild/schema-change suite converges to exact LSN, zero loss, zero duplication, including a 20-pattern DDL matrix.
- W4 Performance/retention: pilot does not revert to ES/CH within term; strong-search p99 ≤ 150 ms at 2x provisioned burst.
- W5 Source safety: bounded slot lag with automatic shed-to-own-log; customer WAL retention never exceeds configured cap.
- G-M Memory: adversarial-mix simulation passes on the single node role (zero kernel OOM; apply floor intact; bounded promises hold).

## Investability case (recorded for fundraising narrative)
Comparables: PeerDB seed→acquisition in ~8 months on the pipe alone; Neon ~$1B / Crunchy $250M Postgres-layer exits; ClickHouse productizing PG CDC to GA within a year of acquisition. Market math: each target customer carries ~1–2 engineers of pipeline load (~$300–500K/yr) plus Kafka/Elastic/CH spend; price at 20–30% of deleted cost; 30–50 customers ≈ Series-A-shaped. Milestones: W1 = seed proof; W2+W3 = Series A; W4 = expansion. Build: 5–8 senior engineers, 9–12 months to pilot grade; long pole is W3, not engines.

## Drawbacks
- Ceiling risk: a layer's ACV is smaller than a database-of-record's; platform upside is deferred behind W-gates.
- Dependency risk: pgoutput surface and PG-major changes; DataFusion/Tantivy governance.
- Competitive squeeze: ClickPipes (analytics-only, into-CH), ParadeDB (search-only, in-PG), managed Debezium (easier pipeline). Differentiation intersection: both shapes + provable LSN + vanilla-PG query surface; occupied by no one.
- Materialize-pattern risk: category confusion/burn — mitigated by selling budget-line replacement ("delete the pipeline stack") and read-only COGS.

## Alternatives considered
1. Continue v0.3 full database (rejected: scope = five companies; zero demand gates; maturity asymmetry risk #9 High).
2. Analytics-only or search-only layer (rejected: each half is occupied — ClickPipes, ParadeDB; the intersection is the open ground).
3. Pipeline product (PeerDB-shaped) (rejected: monetizes the thing we claim to delete; destination vendors own that lane).
4. GPT memo (prior art, adopted with corrections): "no custom MVCC" imprecise — snapshot-correct visibility at an LSN is unavoidable and already measured (T5); two pilot-killers it missed are now gates W5 (slot safety) and the DDL matrix inside W3; two of its seven "distractions" (custom optimizer, multi-region) were never planned.

## Unresolved questions (ranked; new risk register head)
1. Extension-surface spike: can LSN-consistent custom scans + freshness machinery live entirely in extension APIs on stock PG, without core patches? (New technical risk #1 — needs a spike before any code.)
2. DDL capture design: event-trigger coverage vs. WAL-message sidechannel vs. catalog diffing; the 20-pattern matrix definition.
3. Snapshot-consistent initial load at scale (consistent-point snapshot + catch-up handoff) for multi-TB sources.
4. Freshness SLO sizing model under sustained (not burst) overload — promoted from old risk #5.
5. Pricing model that survives community audit; open-source line (engine open, cloud closed?).
6. G-M simulation execution (old risk #8).

## Trial evidence carried
T3 (replay-lag), T4 (rebuild RTO), T5 (DataFusion visibility finding, ADR-002 basis), memory spec L-items (single role), G-M. Shelved-but-valid: T1, T2, T6 (platform hypothesis only).
