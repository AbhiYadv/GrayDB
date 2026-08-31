# RESEARCH-R1 — GrayDB vs ClickHouse under continuous CDC (adopted 2026-08-17)

Source: founder-relayed architect feedback (sol). Status: **adopted as Research Target #1**.
Architecture freeze holds: nothing below changes the design — it implements designed-but-
stubbed pieces and then measures.

## The uncomfortable baseline (accepted as true until disproven)

On a static OLAP benchmark, ClickHouse wins. MergeTree's sorted immutable parts, sparse
granule index, vectorized parallel execution, and a decade of format-specific optimization
are the machine we are facing. "Columnar + DataFusion" is table stakes, not an advantage.

## Hypothesis (falsifiable)

> GrayDB can deliver **materially lower and more stable** analytical query latency than
> ClickHouse when the source PostgreSQL workload contains continuous inserts, updates and
> deletes — while maintaining exact source-LSN visibility.

Rationale: GrayDB's visibility (`insert_lsn <= L && !deleted_by(<= L)`) is a native
predicate over immutable segments + delete sidecars — versioning is free at write time and
cheap at read time. ClickHouse's mutable-data answers (ReplacingMergeTree+FINAL, and since
2025 lightweight-update patch parts, ~7–18% reported query overhead) pay at query or merge
time. The open question is whose price is lower **under sustained churn**.

## Kill criterion

If ClickHouse's heavy-CDC latency stays flat (or degrades less than ours) at equal
correctness, the hypothesis is DEAD and we say so in this file. No architecture changes
either way until this is settled.

## The interesting shape of a win (from the feedback, verbatim intent)

```
                quiet        heavy CDC
ClickHouse       60 ms         350 ms
GrayDB           75 ms          90 ms
```
Stability under churn — not a 10 ms scan race.

## Protocol

Workload: `orders` table, continuous 90% INSERT / 8% UPDATE / 2% DELETE, while running:

  Q1: SELECT customer_id, sum(amount), count(*) FROM orders
      WHERE created_at >= now() - interval '7 days' GROUP BY customer_id
  Q2: SELECT status, count(*) FROM orders WHERE <tenant predicate> GROUP BY status

Freshness requirement: < 1 s. Equal correctness requirement on BOTH systems: results must
be exact at the measured source LSN — no stale-vs-fresh comparisons, no un-deduplicated
ClickHouse reads vs exact GrayDB reads.

Metrics per phase (quiet / heavy-CDC): source→visible latency, query p50/p95/p99,
CPU/query, bytes read, memory/query, CDC apply CPU, update amplification, merge/compaction
CPU. Plus continuous correctness probes.

## Scale plan (honest about hardware)

| Stage | Where | Scale | Purpose |
|---|---|---|---|
| R1-local | this Windows laptop | 1–5M rows seed, 200–1000 tps CDC | harness bring-up, GrayDB column, find our own cliffs |
| R1-full | Linux box (same one SP6b needs) | 1B rows, sustained CDC | the real GrayDB-vs-ClickHouse table |

ClickHouse has no native Windows build — the head-to-head REQUIRES the Linux stage.
Local numbers are for finding and fixing GrayDB's own cliffs, never for marketing.

## Prerequisites found by code inspection (must fix before any number is real)

| # | Current shortcut | Why it poisons the benchmark | Designed replacement |
|---|---|---|---|
| P1 | Studio apply loop re-replays the ENTIRE frame log every 500 ms | O(N²) over a run; measures replay, not apply | `graydb-log` LogTail (charter: `tail(from_lsn)`) + incremental StreamDecoder |
| P2 | Per-tick `finalize()` flushes open rows to a new parquet segment | thousands of tiny segments under churn; measures file-open cost | flush at `columnar.flush_rows` only; fresh rows served from an in-memory overlay (memtable-over-segments, same shape as our staging design) |
| P3 | Reader copies every table into a MemTable per query | latency scales with table size regardless of query | DataFusion TableProvider streaming parquet segments with LSN+bitmap masking and projection pushdown (T5's "visibility predicate inside the scan") |
| P4 | Status `rows_visible` full-scans every table | pollutes CPU during runs | O(1) visible-row counter |

## Status log

- 2026-08-17: adopted; prerequisites identified; implementation begun (P1–P4 + `bench-cdc` harness).
