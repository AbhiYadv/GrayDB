# R1 Phase 1 Task 14: ClickHouse CDC Recovery Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` to execute this plan task by task. Use `superpowers:systematic-debugging` for the failure investigation and `superpowers:test-driven-development` for every behavior change.

**Goal:** Prove and remove the ClickHouse CDC throughput and retry-idempotency failures that invalidated the live 1 GiB Mac rehearsal, then make one guarded acceptance attempt without weakening `R1-P1-v1`.

**Architecture:** First isolate source-writer capacity, ledger wait, ClickHouse request cost, replay boundaries, and exact-LSN visibility in a bounded deterministic diagnostic. Only after the failure is reproduced and attributed, replace transaction-at-a-time ClickHouse application with ordered bounded micro-batches that retain deterministic per-block retry identity. Acknowledge PostgreSQL only through the highest contiguous fully applied and verified source LSN. Preserve every existing correctness and sampling gate.

**Tech stack:** Rust 1.95, Tokio, PostgreSQL 17 logical replication, ClickHouse 25.8 LTS HTTP inserts, existing `graydb-r1` contracts, Docker Compose, JSONL run artifacts.

**Spec:** `docs/superpowers/specs/2026-09-01-r1-phase-1-benchmark-design.md`

**Blocks:** Completion of Task 13 in `docs/superpowers/plans/2026-09-01-r1-phase-1-mac-harness.md`.

## Observed failure evidence

- The initial correctness checkpoint passed with equal GrayDB and ClickHouse Q1-Q5 digests.
- The Cdc300 stage became invalid because it did not obtain the required 30 successful samples for every query and engine.
- ClickHouse physical event uniqueness failed at logical checkpoint 344: `count()=3212206`, `uniqExact(event_id)=3212195`.
- The observed ClickHouse path applied roughly 10 transactions per second while the workload required roughly 250 transactions per second.
- Backlog reached 81,541,616 bytes and freshness p99 reached 41,938 ms during the failed stage.
- Each failed 1 GiB attempt retains roughly 4.3-4.5 GiB of evidence and service data under `/Volumes/Crucial X9/GrayDB/.r1/runs`.

These observations are hypotheses and boundaries, not permission to implement a guessed fix. Task 1 must reproduce and attribute them before Task 2 changes CDC behavior.

## Global constraints

- Benchmark identity remains `R1-P1-v1`; seed remains `20260901`.
- Do not lower Cdc300/Cdc1000 rates, shorten the frozen measured windows, reduce the 30-sample floor, relax exact-LSN comparison, or weaken duplicate/missing/reorder/hash/tombstone checks.
- Do not claim either engine won unless a complete run writes `result.json` with `valid: true` and its checksums verify.
- Do not delete, prune, move, or overwrite existing `.r1/runs` artifacts. Any future pruning command requires a separate design and explicit user approval.
- Do not automatically retry a full 1 GiB rehearsal. One authorized attempt follows all smaller gates; a failure stops execution and preserves evidence.
- Preserve the user's existing uncommitted changes in `.superpowers/sdd/2026-09-01-r1-phase-1-mac-harness/progress.md`, `crates/graydb-studio/src/lib.rs`, and `crates/graydb-studio/src/main.rs`. Do not reset, restore, stage, or commit them as part of Task 14.
- Rust owns the deterministic writer, CDC path, query driver, validation, and measured timing. Shell may orchestrate services but no LLM, Python generator, or interactive agent participates in the timed data path.
- Every production behavior change follows RED-GREEN-REFACTOR. Reports must include the failing test command/output observed before implementation and the passing output after implementation.
- Release builds are mandatory for measured performance. Diagnostic/debug runs are evidence only and cannot produce a performance winner.
- PostgreSQL feedback may advance only through the highest contiguous source LSN whose complete transaction data and application proof are durable in ClickHouse.
- Do not use `received_lsn` as proof of query visibility unless a regression test proves there are no unapplied committed changes or open/pending decoded transactions through that LSN.

## Interface and task dependency scan

| Producer | Consumer | Contract | Ruling |
| --- | --- | --- | --- |
| Task 1 diagnostic counters | Task 2 batch design | Separately measure source generation, ledger wait, ClickHouse requests, apply rate, backlog, and freshness. | Task 2 cannot begin from aggregate elapsed time alone. |
| Task 1 partial-failure reproducer | Task 2 retry identity | Failure occurs after one or more raw-table inserts but before the applied marker. | Preserve the real partial-success boundary; a mock-only retry test is insufficient. |
| Task 2 batch applier | Existing exact-LSN query adapters | Applied/visible LSN cannot overtake the highest contiguous fully applied batch. | Correctness outranks throughput. |
| Task 2 marker batching | PostgreSQL replication feedback | Marker proof and row-block durability precede feedback. | A marker cannot be used as an intent record written before rows. |
| Task 3 gates | Task 4 full rehearsal | Static, live failure-replay, and 120-second Cdc300 diagnostic gates must pass. | No full rehearsal based only on unit tests. |
| Task 4 run | Original Task 13 documentation | Only a valid, checksum-verified result may complete the Mac acceptance claim. | An invalid run remains diagnostic evidence only. |

---

### Task 1: Reproduce and attribute the CDC failure on a bounded dataset

**Owned files:**

- Modify: `crates/graydb-r1/src/clickhouse.rs`
- Modify: `crates/graydb-r1/src/runtime.rs`
- Modify: existing focused test modules or integration tests under `crates/graydb-r1/tests/`
- Create only if needed: one Task 14 diagnostic module under `crates/graydb-r1/src/` or `crates/graydb-r1/tests/`

**Must not modify:** `crates/graydb-studio/src/lib.rs`, `crates/graydb-studio/src/main.rs`, benchmark profile thresholds, result validity rules, or existing run artifacts.

- [ ] Write a failing deterministic test that inserts raw event rows, fails before the applied-transaction marker, reconnects from the last acknowledged LSN, and demonstrates the duplicate-event class through real ClickHouse behavior. The test name must state that replay after a row/marker partial failure must keep physical event IDs unique.
- [ ] Verify RED and record the exact failing assertion and duplicate counts in the task report.
- [ ] Add bounded diagnostic timing/counter collection without changing application semantics. At minimum record source transactions/second, source rows/second, ledger-wait duration, ClickHouse HTTP requests by operation, raw insert duration, marker insert/verification duration, applied transactions/second, applied rows/second, backlog bytes, freshness, retry count, and acknowledged LSN.
- [ ] Run a deterministic 1,000-10,000 transaction diagnostic and isolate source-writer capacity from ClickHouse sink capacity. Do not use a full 1 GiB seed unless the small live fixture cannot reproduce the boundary and the report explains why.
- [ ] Capture retry tokens and block boundaries for the partial failure. Determine whether the replay uses identical tokens, whether ClickHouse accepts those tokens for the affected table engine, and why physical duplicates remain possible.
- [ ] State one proven root-cause hypothesis for the throughput failure and one for the duplicate failure. If either remains unproven, report `BLOCKED` rather than starting Task 2.
- [ ] Keep diagnostic artifacts append-only and redact credentials.

**Acceptance:** The report contains commands, environment identity, before/after counters, the RED failure, exact duplicate counts, retry-token evidence, request-per-transaction evidence, and a causal explanation that distinguishes source-writer limits from sink limits. Instrumentation tests pass, but the deliberate reproduction may remain an expected-failure diagnostic until Task 2.

**Commit:** `test(r1): reproduce clickhouse cdc replay failure`

---

### Task 2: Implement ordered idempotent ClickHouse CDC micro-batches

**Owned files:**

- Modify: `crates/graydb-r1/src/clickhouse.rs`
- Modify: `crates/graydb-r1/src/runtime.rs`
- Modify: `crates/graydb-r1/src/replication.rs` only if the proven contiguous-ACK contract requires it
- Modify: focused unit and live integration tests

- [ ] Starting from the Task 1 reproduction, keep the test RED and implement the smallest architecture that makes replay physically idempotent.
- [ ] Collect a bounded number of complete source transactions while preserving source-LSN and sequence order.
- [ ] Group rows by destination raw table and use a bounded number of ClickHouse inserts per batch rather than per transaction.
- [ ] Give each table block a deterministic retry identity derived from immutable batch contents. Retrying the same partially successful batch must send the identical identity and must not create extra physical rows.
- [ ] Write applied-transaction markers only after every required raw-table block succeeds. Verify the contiguous marker range before acknowledging PostgreSQL.
- [ ] On any gap, reorder, missing sequence, operation-hash change, row-block ambiguity, or marker mismatch, stop application without advancing feedback.
- [ ] Bound rows, bytes, and maximum wait time per batch so low traffic cannot stall visibility indefinitely and high traffic cannot exceed memory limits.
- [ ] Add tests for timeout flush, size flush, ordered multi-transaction flush, partial-table failure, marker failure, process reconnect, same-token retry, changed-content rejection, and highest-contiguous-LSN feedback.
- [ ] Review the existing uncommitted GrayDB Studio `frames == 0` visible-LSN change separately. Do not include it in this task's commit unless its correctness test first fails on committed code and the user-owned edit is reconciled without overwrite.

**Acceptance:** The Task 1 partial-failure test passes against real ClickHouse; `count() = uniqExact(event_id)`; one logical marker exists per source LSN; missing, duplicate, reordered, and hash-changed ledger cases fail closed; feedback never crosses an unverified LSN.

**Commit:** `fix(r1): batch clickhouse cdc with replay safety`

---

### Task 3: Prove correctness and throughput before another full rehearsal

**Owned files:** Test/report artifacts only unless a failing gate returns execution to Task 1 or Task 2 through a reviewed fix round.

- [ ] Run focused `graydb-r1` unit tests and integration-test compilation.
- [ ] Run the Compose contract test.
- [ ] Run live ignored PostgreSQL workload and ClickHouse CDC suites.
- [ ] Run formatting, `cargo clippy -p graydb-r1 --all-targets -- -D warnings`, `cargo test --workspace`, and `git diff --check`.
- [ ] Run a release-built controlled Cdc300 diagnostic for 120 seconds.
- [ ] Require at least 95% of requested source rows/second.
- [ ] Derive the required transaction rate from the frozen transaction mix and require at least 25% measured ClickHouse sink headroom over it.
- [ ] Require backlog to be stable or declining after warm-up, freshness p99 at or below 1,000 ms, at least 30 successful samples for Q1-Q5 on both engines, and zero duplicate/missing/reordered/hash/tombstone failures.
- [ ] Record available disk space and the projected size of one full rehearsal. Fail closed if the existing 20% projected-free-space or 15% runtime-stop policies would be crossed.

**Acceptance:** Every gate passes with terminal-readable and machine-readable evidence. A gate failure returns to diagnosis; it does not relax the benchmark.

**Commit:** `test(r1): prove cdc300 recovery gates`

---

### Task 4: Run one guarded 1 GiB acceptance rehearsal and resume Task 13

- [ ] Print the exact run command, release binary revision, free space, projected consumption, and artifact directory before starting.
- [ ] Run one `mac-smoke` correctness rehearsal with automatic full-run retry disabled.
- [ ] On failure, stop, preserve all artifacts, and report the first invalidation plus the relevant log paths.
- [ ] On success, require Preflight, Seed, BaselineSnapshot, Bootstrap, InitialCheckpoint, Warmup, Quiet, Cdc300, Cdc1000, RateSearch, FailureSequence, FinalCheckpoint, Report, and Checksums to pass.
- [ ] Verify `result.json` has `valid: true`, verify `SHA256SUMS`, and report total and per-stage execution time.
- [ ] Only after those checks, return to original Task 13 documentation and runbook completion.

**Acceptance:** One checksum-verified valid Mac diagnostic result exists. It remains non-publishable and does not establish the official AWS winner.

