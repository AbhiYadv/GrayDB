# SDD ledger — plan: docs/superpowers/plans/2026-09-01-r1-phase-1-mac-harness.md

## Setup

- Worktree: `/Volumes/Crucial X9/GrayDB/GrayDB/.worktrees/r1-phase-1`
- Branch: `codex/r1-phase-1`
- Base before Task 1: `4b9a2ce`
- Authority: `docs/superpowers/specs/2026-09-01-r1-phase-1-benchmark-design.md`
- Plan: `docs/superpowers/plans/2026-09-01-r1-phase-1-mac-harness.md`
- AWS provisioning is excluded until the Mac 100 GiB gate, capacity report, cost review, and explicit later approval.

## Pre-dispatch conflict scan

| Scope | Producer / consumer | Finding | Ruling |
| --- | --- | --- | --- |
| Task 1 ↔ Task 2 | `contracts.rs` → `RunConfig`, `ProfileSpec` / preflight | Shared profile values are compatible. | Keep Task 1 contracts as the sole source of timing and resource values. |
| Task 1 ↔ Task 3 | `LogicalCheckpoint`, seed → generator/query | Same seed and checkpoint types are used. | Query parameters derive from logical checkpoints; no wall-clock values. |
| Task 1 ↔ Task 4 | `ProfileSpec` → size loader | Minimum byte thresholds align with the spec. | Loader stops only after measured `pg_table_size` reaches the threshold. |
| Task 1 ↔ Task 5 | seed, rates → workload planner | Mix and rate values are frozen once. | Task 5 consumes Task 1 values and adds no alternate defaults. |
| Task 1 ↔ Task 6 | checkpoint/LSN → replay map | Numeric LSNs can differ between isolated restores. | Compare logical sequence checkpoints and record per-replay LSN maps. |
| Task 1 ↔ Task 7 | `EngineKind`, checkpoint → adapter | Adapter contract covers both engines. | Both adapters expose the same exact-LSN interface. |
| Task 1 ↔ Task 8 | engine/query contracts → ClickHouse adapter | ClickHouse requires the shared query result shape. | Exact-LSN semantics are implemented behind `EngineAdapter`. |
| Task 1 ↔ Task 9 | checkpoint/IDs → oracle/verdict | Validity depends on frozen identifiers. | Verdict code cannot change profile or benchmark IDs. |
| Task 1 ↔ Task 10 | profiles → metric windows | Timing rows are consumed, not redefined. | Reports use profile data and never select fastest repetition. |
| Task 1 ↔ Task 11 | paths/resources → Compose | Compose must match the 8 CPU / 12 GiB / 600 GiB contract. | Preflight remains the runtime authority; Compose is checked against it. |
| Task 1 ↔ Task 12 | `RunConfig`, stages → controller | Controller state names match the frozen stage sequence. | Resume is stage-atomic and uses the same profile catalog. |
| Task 1 ↔ Task 13 | CLI/profile → recipes | Recipes invoke release `r1ctl` with named profiles. | No recipe creates AWS resources. |
| Task 2 ↔ Task 4 | `RunDirectory`, `EventSink` → loader | Loader artifacts need append-only event and atomic manifest writes. | Task 4 writes only through Task 2 artifact interfaces. |
| Task 2 ↔ Task 5 | event sink → writer/ledger | Intent and commit events must be visible and redacted. | Ledger never writes credentials or unredacted SQL URLs. |
| Task 2 ↔ Task 10 | event sink → metrics/report | Reports need raw events and checksums. | Metrics are append-only JSONL and included in `SHA256SUMS`. |
| Task 2 ↔ Task 12 | run lock/state → controller | Resume must not overlap another controller. | One exclusive lock per run root; incomplete state is resumable. |
| Task 3 ↔ Task 4 | generator/COPY → measured loader | Prefix generation must be reproducible at each scale. | Loader uses only deterministic Task 3 batches. |
| Task 3 ↔ Task 5 | `draw`/row types → operation planner | Application operations must not use a second RNG. | Task 5 reuses Task 3's SplitMix64 function. |
| Task 3 ↔ Task 6 | operation rows → SQL/replay | Replays require complete operation values. | Intent records contain full typed operations, not re-generated choices. |
| Task 3 ↔ Task 8 | queries/digests → ClickHouse SQL | Engine-specific SQL must preserve logical Q1-Q5. | ClickHouse SQL is checked against the shared canonical digest. |
| Task 3 ↔ Task 9 | canonical result → oracle | Result hashing must be identical across engines. | One encoder and digest implementation is shared. |
| Task 3 ↔ Task 10 | query IDs/results → metrics | Metric keys need stable query names. | Query IDs are an enum, not free-form labels. |
| Task 4 ↔ Task 6 | dataset manifest → baseline/replay | Replays must use the same dataset identity. | Manifest hash is a hard precondition for replay. |
| Task 5 ↔ Task 6 | ledger → source LSN mapper | Unknown commit recovery is a boundary risk. | Control-table marker resolution precedes any retry. |
| Task 6 ↔ Task 8 | pgoutput/typed changes → ClickHouse sink | Both consumers need transaction-complete ordering. | Reuse `StreamDecoder` and acknowledge only after durable sink state. |
| Task 7 ↔ Task 8 | `EngineAdapter` → GrayDB/ClickHouse | Adapters must report the same visibility proof fields. | Adapter tests require target-LSN proof before accepting results. |
| Task 7 ↔ Task 9 | `QueryResult` → correctness | GrayDB HTTP output must be canonicalizable. | Preserve strings and nulls; do not parse monetary values as floats. |
| Task 8 ↔ Task 9 | version/tombstone → exact oracle | MergeTree physical state cannot substitute for logical state. | `argMax`/version reduction is validated at every checkpoint. |
| Task 8 ↔ Task 12 | service lifecycle → failure stages | ClickHouse restart must not lose or duplicate changes. | Controlled failure uses Compose operations and the same ledger hash. |
| Task 9 ↔ Task 10 | verdict → report | Invalidity must suppress winner language. | `ReportWriter` rejects winner text for every invalid result. |
| Task 10 ↔ Task 12 | timers/metrics → controller | Every stage needs start/end and operation timings. | Controller emits stage boundaries before and after each operation. |
| Task 10 ↔ Task 13 | report → runbook/recipes | Operators need absolute artifact paths and total time. | Task 13 documents the artifact contract without committing generated data. |
| Task 11 ↔ Task 12 | Compose → controller | Controller must start/stop only named R1 services. | Argument-vector commands use the dedicated Compose project and run root. |
| Task 12 ↔ Task 13 | CLI → justfile/runbook | Recipes must match actual subcommands. | Task 13 runs CLI help and Compose smoke before documentation commit. |

### Task self-consistency rows

| Task | Own-text check |
| --- | --- |
| Task 1 | Tests load the TOML file and assert the exact profile values the task creates. |
| Task 2 | Tests cover redaction, projected free-space rejection, append-only artifacts, and the files the task modifies. |
| Task 3 | Tests cover deterministic bytes, checkpoint parameters, query assets, and canonical hashing. |
| Task 4 | Tests cover threshold stopping, timestamp-independent identity hashes, and compile the PostgreSQL integration target. |
| Task 5 | Tests cover 90/8/2 mix, hash-chain corruption, unknown commit, and rate limiting. |
| Task 6 | Tests cover commit-end LSN mapping, replay sequence continuity, and uncertain-commit recovery. |
| Task 7 | Tests cover HTTP proof mismatch and the configurable Studio bind. |
| Task 8 | Tests cover version ordering, tombstone visibility, retry deduplication, exact SQL, and restart integration. |
| Task 9 | Tests cover missing, duplicate, stale, tombstone, mismatch, and winner-rule mutations. |
| Task 10 | Tests cover histograms, max values, invalid reports, resource samples, and AWS estimate output. |
| Task 11 | Tests cover service names, health checks, bind mounts, resources, and the Colima script syntax. |
| Task 12 | Tests cover stage-atomic resume, planned failures, stop rules, baseline isolation, and CLI commands. |
| Task 13 | Recipes, docs, static checks, service checks, the 1 GiB rehearsal, artifact verification, and no-cloud AWS estimate are all specified. |

## Rulings

- Ruling: use a linked worktree on `codex/r1-phase-1` — the approved execution plan requires isolation and preserves `main`; cost if wrong: extra branch cleanup work.
- Ruling: keep service-dependent integration tests ignored until Task 11 creates the Compose environment — this preserves task independence without weakening the tests; cost if wrong: integration failures surface later than unit failures.
- Ruling: run the controller on the host while databases run in Compose — the user asked to see terminal logs, and this keeps the data-plane services independently killable; cost if wrong: host controller resource usage must be captured for fairness.

## Dispatch

- Task 1 implementer: `/root/r1_task1_contracts` (`gpt-5.4-mini`, medium), brief `task-1-brief.md`, report `task-1-report.md`, BASE `4b9a2ce`.
- Task 2 implementer: `/root/r1_task2_artifacts` (`gpt-5.4-mini`, medium), brief `task-2-brief.md`, report `task-2-report.md`, BASE `4bac4e3`.
- Task 2 reviewer: `/root/r1_task2_review` (`gpt-5.5`, medium), package `review-4bac4e3..e5519e7.diff`, FAIL; fix round 1 dispatched to original implementer.

## Task status

- Task 1: complete (commits `03ccc09`..`4bac4e3`, review clean after fix round 1)
- Task 2: in progress (implementation `e5519e7`; review found runtime-resource preflight and task-local formatting gaps; fix round 1 active)
- Task 3: pending
- Task 4: pending
- Task 5: pending
- Task 6: pending
- Task 7: pending
- Task 8: pending
- Task 9: pending
- Task 10: pending
- Task 11: pending
- Task 12: pending
- Task 13: pending

## Task 1 execution

- Implementer status: complete; commit `03ccc091fc365bd55d8552e535d05c8e03c153c1`.
- Focused contract tests: PASS.
- Workspace test: baseline failure in `graydb-log::tail_tests::tail_reads_incrementally_and_survives_resume_truncation`; unrelated to Task 1 according to implementer report.
- Review package: `review-4b9a2ce..03ccc09.diff`.
- Task reviewer: `/root/r1_task1_review` (`gpt-5.5`, medium), complete; initial review failed and led to fix round 1.

- Task 1 review: FAIL. Blocking findings: frozen workload mix `90/8/2` missing from typed/profile contract; contract test omits benchmark ID, seed, limits, rates, and four Mac profile rows.
- Task 1: fix round 1/5 dispatched to original implementer `/root/r1_task1_contracts`; scoped re-review will use the fix range after its report.
- Fix commit: `4bac4e3bca5530b24eb5e13d206cf8efcfc2c756`; fix report appended with focused test evidence.
- Scoped re-review: `/root/r1_task1_rereview` (`gpt-5.4`, medium), package `review-03ccc09..4bac4e3.diff`, complete.
- Scoped re-review verdict: PASS for spec compliance and task quality; both findings ADDRESSED; no new breakage.
