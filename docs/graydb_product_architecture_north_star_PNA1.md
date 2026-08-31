# GrayDB — Product North Star Architecture (PNA-1.0) — LOCKED

Status: LOCKED. This is the one architecture. It does not have versions v0.3/v0.4 — those labels are hereby reclassified as *activation states and build sequencing* under this single drawing. Changing THIS document requires an RFC that defeats the constitution (§3) with new evidence. Changing build order does not touch this document.

## 1. What GrayDB is (final, one sentence)
GrayDB is a PostgreSQL-native derived database that turns one durable LSN-ordered log into every read shape a Postgres estate needs — compressed columnar analytics, BM25+vector search, and (Act 3) a writable row store with owner-first sharding — served through stock PostgreSQL heads where every query declares its consistency class and can prove the exact LSN it reflects.

## 2. The full component inventory (the whole building, drawn once)

DATA PLANE
- Source PostgreSQL (customer truth; sole writer until Act 3)
- Attach pack (publication + event-trigger DDL capture; SQL objects only)
- Ingest decoders (pgoutput; WAL-budget ladder; fresh-session raw-capture rule)
- Durable LSN log (v1: single-writer, S3-gated ack · Act 2: quorum log service — the v0.3 log design, banked)
- Materializer shapes: Columnar (DataFusion, compressed S3 segments + delete bitmaps) · Search (Tantivy BM25 + HNSW) · Row shape (Act 3; heap-compatible, undo-engine option banked)
- Reader heads (stock PG + graydb extension: LSN-visibility custom scans, @@@ operator, consistency GUCs)
- Writer heads (Act 3; owner-first execution; T2/T6 trial evidence banked)
- Gateway/pooler (Act 2+ at multi-tenant scale)

BACKGROUND SERVICES (PG-analog mapping is normative)
- Control supervisor (postmaster analog) · Schema registry (system-catalog analog, LSN-versioned) · Snapshotter (checkpointer analog; harness-validated bases) · Compactor (autovacuum analog; merges + bitmap GC) · Segment flusher (bgwriter analog) · Segment tiering (archiver analog) · Freshness telemetry (stats analog) · Memory broker (shared_buffers/work_mem replacement; grants, pools, ladder, L1–L15) · Egress replication (logical walsender analog; the exit hatch)

CONTROL PLANE
- Catalog + placement maps · Contention governor (Act 3) · Autoscaler (T7 policy) · Correctness harness (a component, not QA; watches from the first byte)

STORAGE
- Object storage (segments, snapshots, log, branch DAG — truth alongside the log) · NVMe staging (never truth)

## 3. Constitution (unchanged, governs everything)
I1 source of truth is external and singular until Act 3 activates writes by explicit gate. I2 one durable LSN-ordered log is the spine; every shape derivable from it (retention algebra: rebuildable = validated base + log[B→head]). I3 shapes are disposable; snapshot+replay reconstructs anything; slot ack gated on remote durability. I4 every query answers under a declared consistency class and can prove its LSN (strong = source barrier; read_your_writes(token); bounded vs heartbeat; eventual). I5 the customer's database is sacred: SQL-objects-only footprint, hard WAL budget + ladder, capped read impact.

## 4. Activation matrix (phases activate; they never redraw)
| Component | Act 1 (wedge) | Act 2 (platform) | Act 3 (system of record) |
|---|---|---|---|
| Attach pack, ingest, durable log (single-writer), registry, columnar, search, reader heads, snapshotter, compactor, tiering, flusher, telemetry, broker, harness, egress | ACTIVE | active | active |
| Quorum log service, gateway, shape replicas (regional reads), branching/scale-to-zero | drawn, dormant | ACTIVATES | active |
| Row shape, writer heads, owner-first sharding, governor | drawn, dormant | dormant | ACTIVATES |
Activation triggers: Act 1→2 = W-gates passed + paying pilots converted. Act 2→3 = platform revenue + explicit RFC re-opening the write path against I1 with market evidence.

## 5. Governance rule (the fix for the architect's named failure)
- This architecture is stable. External reviews, pivots, and sequencing rulings modify the ACTIVATION MATRIX and build order only.
- Any change to §2/§3 requires: an RFC, evidence that defeats the current position, and explicit founder ratification. Mood, fashion, or a reviewer's framing is insufficient.
- The architect defends this document against everyone — including the founder and including future reviews — unless the evidence bar is met. Agreement is not a service; evidence adjudication is.
- Component-level specs remain: wedge spec v0.4.1 (Act-1 implementation detail + W/S gates), memory architecture v0.1 (broker), RFC-0004 (why Act 1 ships first). They are children of this document, not rivals.

## 6. Supersession note
All prior language implying the architecture changed between "v0.3" and "v0.4" is corrected: the building never changed; the construction schedule did. v0.3 artifacts = the Act-2/3 wings' design record with trial evidence (T1, T2, T6 banked). v0.4.1 artifacts = Act-1 construction documents.
