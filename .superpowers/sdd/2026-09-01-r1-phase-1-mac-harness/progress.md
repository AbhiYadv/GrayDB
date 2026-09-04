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
- Ruling: treat the Task 3 distribution and JSON ordering gaps as load-bearing — Task 4 manifests and later correctness reports depend on the frozen generator shape matching spec section 4.2; cost if wrong: extra generator rework if the final re-review had already accepted the current implementation.
- Ruling: repair the interrupted uncommitted Task 9 syntax without committing it before resuming Task 6 verification — the shared worktree cannot compile Task 6 while `oracle.rs` has an unmatched delimiter; cost if wrong: temporary Task 9 code is exercised before its own formal task review, but it remains uncommitted and will still enter the full Task 9 loop.
- Ruling: replace the plan's primary-key-only `ReplacingMergeTree(_version)` layout with a version-preserving `MergeTree ORDER BY (primary_key, _version)` design — spec section 8 permits an equivalent version-preserving MergeTree and exact historical LSN reads cannot survive replacement merges otherwise; cost if wrong: ClickHouse storage amplification and comparison characteristics differ from the plan's literal DDL choice.
- Ruling: move `invalid_result(reason)` to Task 10's `report` test module when the real `RunResult` exists and remove Task 9's reduced production surrogate — the plan itself assigns `RunResult` to Task 10, so Task 9 cannot honestly construct the complete zero-metric result yet; cost if wrong: Task 9 loses a local fixture until Task 10 lands.
- Ruling: keep the Compose contract target at repository-relative `bench/r1/compose.yml`, not the pre-existing product `docker-compose.yml` — Task 11 explicitly creates the isolated R1 Compose environment there and must not validate or mutate the unrelated product stack; cost if wrong: Compose parsing remains intentionally unexecutable until Task 11 creates the file.
- Ruling: make Task 11 expose a safe host `r1ctl` build hook that defers until Task 12 creates the binary, rather than adding a duplicate placeholder CLI — Task 12 owns `src/bin/r1ctl.rs` and a fake binary would create an invalid integration seam; cost if wrong: the first Task 11-only environment run cannot build the host CLI until Task 12 is complete.

## Dispatch

- Task 1 implementer: `/root/r1_task1_contracts` (`gpt-5.4-mini`, medium), brief `task-1-brief.md`, report `task-1-report.md`, BASE `4b9a2ce`.
- Task 2 implementer: `/root/r1_task2_artifacts` (`gpt-5.4-mini`, medium), brief `task-2-brief.md`, report `task-2-report.md`, BASE `4bac4e3`.
- Task 2 reviewer: `/root/r1_task2_review` (`gpt-5.5`, medium), package `review-4bac4e3..e5519e7.diff`, FAIL; fix round 1 dispatched to original implementer.
- Task 2 fix commit: `74e1358`; scoped re-review `/root/r1_task2_rereview` (`gpt-5.4`, medium), package `review-e5519e7..74e1358.diff`, PASS; no new in-scope regressions.
- Task 3 implementer: `/root/r1_task3_generator` (`gpt-5.6-luna`, medium), brief `task-3-brief.md`, report `task-3-report.md`, BASE `74e1358`.
- Task 3 reviewer: `/root/r1_task3_review` (`gpt-5.5`, medium), package `review-74e1358..164e99d.diff`, FAIL; fix round 1 dispatched to original implementer.
- Task 3 fix commit: `6c8a0e6`; scoped re-review `/root/r1_task3_rereview` (`gpt-5.4`, medium), package `review-164e99d..6c8a0e6.diff`, pending.
- Task 3 re-reviewer `/root/r1_task3_rereview` and replacement `/root/r1_task3_rereview_alt` errored on the platform usage limit; retry `/root/r1_task3_rereview_retry` (`gpt-5.4-mini`, low) completed a scoped review and found one remaining P1 canonical-column-order gap; fix round 2 dispatched to original implementer.
- Task 3 fix round 2 commit `58f7271` addressed canonical permutation normalization, but the implementer self-identified a concrete cycle relationship violation; fix round 3 dispatched before re-review.
- Task 3 fix round 3 commit `a9b1032`; cumulative scoped re-review `/root/r1_task3_rereview_final` (`gpt-5.5`, medium), package `review-164e99d..a9b1032.diff`, pending.
- Task 3 fix round 4: final scoped re-review artifact did not materialize after resume; controller inspection found generator distribution and JSON ordering gaps against spec section 4.2; fresh implementer will own the fix from BASE `a9b1032`.
- Task 3 fix round 4/5 (1 addressed, 1 open — JSON ordering/dictionary distribution addressed; tenant activity Zipf remains metadata-only; commits `a9b1032`..`5b463e7`).
- Task 3 fix round 5: fresh escalated implementer will address tenant activity skew in actual generated ownership while preserving prefix-safe referential validity from BASE `5b463e7`.
- Task 3 fix round 5/5 (1 addressed, 0 open — tenant activity Zipf now controls generated customer/order/event ownership; commits `5b463e7`..`c6078b2`).
- Task 3 scoped re-review round 5: `/root` delegated via Codex task `01a05c85-a72b-7d02-8229-21f809b3e92c` (`gpt-5.6-terra`, medium), package `review-5b463e7..c6078b2.diff`, PASS.
- Task 4 implementer: Codex task `01a05c87-998d-7f11-9a9a-36b05a727d25` (`gpt-5.6-terra`, medium), brief `task-4-brief.md`, report `task-4-report.md`, BASE `c6078b2`.
- Task 4 reviewer: `/root/r1_task4_review` (`gpt-5.4`, medium), package `review-c6078b2..0bebe37.diff`, pending.
- Task 4 initial review: FAIL; P1 real manifest identity/per-table metrics missing and P2 cumulative `analyze_ms` understated. Fix round 1 sent to original Codex implementer task `01a05c87-998d-7f11-9a9a-36b05a727d25`.
- Task 4 fix commit: `5b3d0ee`; scoped re-review `/root/r1_task4_rereview` (`gpt-5.4-mini`, low), package `review-0bebe37..5b3d0ee.diff`, pending.
- Task 4 scoped re-review: PASS; real identity/size metadata and cumulative timing are addressed with no new in-scope findings.
- Task 5 implementer: `/root/r1_task5_workload` (`gpt-5.6-luna`, medium), brief `task-5-brief.md`, report `task-5-report.md`, BASE `5b3d0ee`.
- Task 5 reviewer: `/root/r1_task5_review` (`gpt-5.4`, medium), package `review-5b3d0ee..8f91358.diff`, initial FAIL recorded locally because the review artifact did not materialize.
- Task 5 fix round 1: local root fix from `8f91358` to current HEAD corrected frozen transaction sizes, schema-ready operation payloads, per-table routing, and minute-bucket rate accounting.
- Task 5 scoped re-review: PASS after fix round 1; workload and ledger suites are green, and only the pre-existing `graydb-registry` clippy warning remains outside scope.
- Task 5 process correction: reviewer candidate `417d8d0` was independently reimplemented/hardened by original worker in `225e32b`; cumulative scoped re-review `/root/r1_task5_rereview` (`gpt-5.4-mini`, low), package `review-8f91358..225e32b.diff`, PASS; no new in-scope gaps.
- Task 6 implementer: `/root/r1_task6_replication` (`gpt-5.6-terra`, high), brief `task-6-brief.md`, report `task-6-report.md`, BASE `225e32b`.
- Task 6 reviewer: `/root/r1_task6_review` (`gpt-5.5`, medium), package `review-225e32b..863e3ac.diff`, pending.
- Task 6 first reviewer errored on usage limit; retry `/root/r1_task6_review_retry` (`gpt-5.4-mini`, low) dispatched on the same package.
- Task 6 retry review: FAIL; fresh-connection unknown-commit recovery and realistic pipeline integration coverage required. Fix round 1 dispatched to original implementer `/root/r1_task6_replication`.
- Task 6 fix round 1 commit `298bcb0`; scoped re-review `/root/r1_task6_rereview` (`gpt-5.4-mini`, low), package `review-863e3ac..298bcb0.diff`, PASS; both findings addressed, no new breakage.
- Task 7 implementer: `/root/r1_task7_adapter` (`gpt-5.6-luna`, medium), brief `task-7-brief.md`, report `task-7-report.md`, BASE `298bcb0`; owns interrupted candidate but must commit only Task 7 files.
- Task 7 reviewer: `/root/r1_task7_review` (`gpt-5.4-mini`, low), package `review-298bcb0..d9681ff.diff`, pending; unrelated formatter spill restored before review.
- Task 7 review: FAIL; structured target+visible LSN proof and pure Studio bind parsing required. Fix round 1 dispatched to original implementer `/root/r1_task7_adapter`.
- Task 7 fix round 1 commit `d313c74`; scoped re-review `/root/r1_task7_rereview` (`gpt-5.4-mini`, low), package `review-d9681ff..d313c74.diff`, PASS; both findings addressed with structured proof validation and process-local bind tests.
- Task 8 implementer: `/root/r1_task8_clickhouse` (`gpt-5.6-sol`, ultra), brief `task-8-brief.md`, BASE `d313c74`; owns the interrupted ClickHouse candidate and must preserve uncommitted Task 9 files.
- Task 8 first implementer terminated on platform usage limit before producing a commit or report; recovery implementer `/root/r1_task8_recovery` (`gpt-5.6-terra`, high) dispatched from the same BASE and ownership boundary.
- Task 8 recovery implementation committed `bfa02b5`; focused ClickHouse tests 14/14 and ignored integration compile passed. Reviewer `/root/r1_task8_review` (`gpt-5.5`, high), package `review-d313c74..bfa02b5.diff`, pending.
- Task 8 review: FAIL; critical history-loss layout plus important tombstone-duplicate and real `StreamDecoder` boundary gaps. Fix round 1 dispatched to `/root/r1_task8_recovery`; DDL conflict ruled in favor of spec section 8.
- Task 8 fix round 1/5 (3 addressed, 0 open — version-preserving raw history, all-physical-row event duplicate validation, and transaction-complete `StreamDecoder` boundary; commits `bfa02b5`..`5ce4134`).
- Task 8 scoped re-review `/root/r1_task8_rereview` (`gpt-5.4-mini`, medium), package `review-bfa02b5..5ce4134.diff`, PASS; no new Critical/Important breakage.
- Task 9 implementer: `/root/r1_task9_oracle` (`gpt-5.6-terra`, high), brief `task-9-brief.md`, BASE `5ce4134`; formally owns and must audit the recovered uncommitted oracle/verdict candidates.
- Task 9 implementation committed `4d2fb6d`; focused oracle/verdict tests 18/18, package check, and formatting passed. Reviewer `/root/r1_task9_review` (`gpt-5.5`, high), package `review-5ce4134..4d2fb6d.diff`, pending.
- Task 9 review: FAIL; critical missing ledger-vs-PostgreSQL aggregate comparison, important reorder/invalidation propagation gaps, and two cross-task ownership findings. Fix round 1 dispatched to `/root/r1_task9_oracle` with rulings for Task 10 `RunResult` ownership and Task 11 R1 Compose path.
- Task 9 fix round 1/5 (5 addressed, 1 parked by prior ruling — dual aggregate comparison, reorder detection, typed invalidations, Task10 helper ownership, and query alias; future R1 Compose path remains intentionally at `bench/r1/compose.yml`; commits `4d2fb6d`..`6242efa`). Scoped re-review `/root/r1_task9_rereview` (`gpt-5.4-mini`, medium), package `review-4d2fb6d..6242efa.diff`, PASS; no new Critical/Important breakage.

## Task status

- Task 1: complete (commits `03ccc09`..`4bac4e3`, review clean after fix round 1)
- Task 2: complete (commits `e5519e7`..`74e1358`; scoped re-review PASS after fix round 1)
- Task 3: complete (commits `164e99d`..`c6078b2`, review clean after fix round 5)
- Task 4: complete (commits `0bebe37`..`5b3d0ee`; scoped re-review PASS after fix round 1)
- Task 5: complete (commits `8f91358`..`225e32b`; cumulative re-review PASS)
- Task 6: complete (commits `863e3ac`..`298bcb0`; scoped re-review PASS after fix round 1)
- Task 7: complete (commits `d9681ff`..`d313c74`; scoped re-review PASS after fix round 1)
- Task 8: complete (commits `bfa02b5`..`5ce4134`; scoped re-review PASS after fix round 1)
- Task 9: complete (commits `4d2fb6d`..`6242efa`, 1 parked by ruling; scoped re-review PASS after fix round 1)
- Task 10 implementer: pending dispatch from BASE `6242efa`; brief `task-10-brief.md`.
- Task 10 implementation committed `ce14c58`; metrics/report tests 4/4 and package check passed, with workspace formatting drift reported outside scope. Reviewer `/root/r1_task10_review` (`gpt-5.4`, medium), package `review-6242efa..ce14c58.diff`, pending.
- Task 10 review: FAIL; P1 raw metrics pipeline/artifact output absent, P1 AWS request loses measured-versus-scaled values, and P2 Markdown omits required evidence fields. Fix round 1 dispatched to original implementer `/root/r1_task10_metrics`.
- Task 10 fix round 1/5 (3 addressed, 0 open — raw metrics collectors/artifact, measured-vs-scaled AWS capacity, and complete Markdown evidence; commits `ce14c58`..`15a3439`). Scoped re-review `/root/r1_task10_rereview` (`gpt-5.4-mini`, medium), package `review-ce14c58..15a3439.diff`, pending.
- Task 10 scoped re-review: original 3 findings addressed, but new P1 found raw metrics serialized into summary `result.json`; fix round 2 dispatched to original implementer `/root/r1_task10_metrics`.
- Task 10 fix round 2/5 (1 addressed, 0 open — raw metrics now serde-skipped from result summary with artifact regression; commits `15a3439`..`20bf2e5`). Scoped re-review `/root/r1_task10_rereview2` (`gpt-5.4-mini`, low), package `review-15a3439..20bf2e5.diff`, pending.
- Task 10 complete (commits `ce14c58`..`20bf2e5`; 2 fix rounds, scoped re-reviews PASS).
- Task 11 implementer: pending dispatch from BASE `20bf2e5`; brief `task-11-brief.md`.
- Task 11 implementation committed `0f98db4`; bash/Compose contract/Rustfmt/diff checks passed; Docker Compose unavailable so live gates deferred. Reviewer `/root/r1_task11_review` (`gpt-5.4`, medium), package `review-20bf2e5..0f98db4.diff`, pending.
- Task 11 review: FAIL; P1 running-profile shape/context validation and host `r1ctl` build hook missing, P2 Compose contract omits ports/images/anonymous-volume assertions. Fix round 1 dispatched to `/root/r1_task11_compose`.
- Task 11 fix round 1/5 (3 addressed, 0 open — profile shape/context validation, safe deferred host CLI build hook, and complete Compose contract; commits `0f98db4`..`ad92092`). Scoped re-review `/root/r1_task11_rereview` (`gpt-5.4-mini`, medium), package `review-0f98db4..ad92092.diff`, pending.
- Task 11 complete (commits `0f98db4`..`ad92092`; scoped re-review PASS after fix round 1; Compose CLI/live services deferred by environment availability).
- Task 12 implementer: pending dispatch from BASE `ad92092`; brief `task-12-brief.md`.
- Task 10: pending
- Task 10: complete (commits `ce14c58`..`20bf2e5`; scoped re-reviews PASS after fix rounds 1-2)
- Task 11: in progress (implementation committed; review pending)
- Task 11: complete (commits `0f98db4`..`ad92092`; scoped re-review PASS after fix round 1)
- Task 12 implementation committed `cda8cb3`; controller/failure/CLI focused tests 12/12, all-target check and scoped formatting/diff checks passed. Reviewer `/root/r1_task12_review` (`gpt-5.5`, high), package `review-ad92092..cda8cb3.diff`, pending.
- Task 12 review: FAIL; two Critical lifecycle/replay omissions, four Important CLI/failure/durability gaps, and one Minor scheduler coverage gap. Fix round 1 dispatched to original implementer `/root/r1_task12_controller`.
- Task 12 fix round 1/5 (7 addressed, 0 open — runtime lifecycle, isolated replay, invalid archival, real mutation fixtures, failure evidence, durable starts, and scheduler/stop coverage; commits `cda8cb3`..`47d6185`). Scoped re-review `/root/r1_task12_rereview` (`gpt-5.4`, high), package `review-cda8cb3..47d6185.diff`, pending.
- Task 12 scoped re-review: findings 3/4/6 addressed; 1/2/5/7 partial, with new Important persisted-plan-config and real-elapsed-duration regressions. Fix round 2 dispatched to original implementer `/root/r1_task12_controller`.
- Task 12 fix round 2/5 (2 addressed, 0 open for its targeted regressions — persisted plan configuration and real elapsed duration; commits `47d6185`..`edf0315`). Scoped re-review could not complete due platform usage limit; authoritative review identified remaining runtime/lifecycle gaps, so round 3 is dispatched to the original implementer.
- Task 12 fix round 3: original implementer reported BLOCKED because the concrete multi-engine runtime bridge is absent; no changes made. Per SDD escalation, fresh architecture worker `/root/r1_task12_runtime_escalation` (`gpt-5.6-sol`, ultra) owns fix round 4 from BASE `edf0315`.
- Task 12 fix round 4: escalation worker hit the platform usage limit mid-round, leaving uncommitted runtime work; controller took over, audited, and completed the round. Concrete `runtime::MacComposeRuntime` (all 15 stages, measured Q1-Q5 windows, rate search, isolated replay binding, exit-75 restart) is now wired into every `r1ctl` subcommand, replacing the fail-closed adapter; legacy `plan: None` runs are invalidated with an exact reason and archived via report/checksums only; writer-restart proof validates missing/duplicate/reorder/catch-up on both engines.
- Task 12 round-4 scoped re-review: FAIL; prior findings 1-5 verified fixed, but new Critical per-engine correctness checkpoint capture raced the writer (deterministic Cdc300 abort), and Important exit-75 never reached the process boundary. Fix: single-capture ledger-derived shared checkpoint for correctness mode (regression test with racy fake), `exit_code_for` maps RestartRequired→75 in `r1ctl` main; minors — planless archived results carry `profile: None`, rate ladder deduplicated into `controller::search_rates`, parked received-LSN and freshness p50/p95 with explicit comments. Round-4b scoped re-review: PASS (all fixes verified, no new breakage; two Minor notes addressed or annotated).
- Final gate evidence: workspace 33 suites 0 failures; lib 87 + bin 1; controller_state 12; runtime_lifecycle 3; r1ctl 4; fmt PASS; clippy 34 warnings identical to edf0315 baseline (0 from round-4 code); check --all-targets PASS; git diff --check PASS; release `self-test-invalidations` PASS. Live 1 GiB rehearsal remains separately authorized.
- Parked-gap fix round (`de4863b`): fixed three gaps that would have distorted the measured benchmark. (1) Dead freshness gate — Studio exposes no `lag_ms`, so freshness p99 was always 0 and the spec's p99>1000ms stop rule could never fire; freshness is now computed in real ms from ledger commit times per query/rate-observation/report percentile. (2) O(entire-file) ledger/map rescans per query iteration and per 20ms CDC poll — replaced with byte-offset incremental `refresh()` plus cached read views. (3) `received_lsn` was fabricated from `applied_lsn` — now parsed from Studio's real field. Scoped re-review: PASS; 8 new tests; all gates green (95 lib / 12 / 3 / 4 / 1; fmt, clippy baseline-identical, diff-check PASS).
- Live environment round (`3d9051a`): Colima r1 profile is live (8 CPU/12 GiB/600 GiB on the external disk; spaced-path virtiofs share requires lima >= 2.2.0) and all three Compose services are healthy. All deferred `#[ignore]`d integration suites executed against real services and PASS: postgres_dataset (two 64 MiB loads, identical content hash), postgres_workload (100 txns + unknown-commit recovery + replay integrity), clickhouse_cdc (exact-at-LSN roundtrip, pgoutput buffering, retry/restart dedup). Five live-only defects fixed: content hash poisoned by per-load WAL position/physical bytes; jsonb `String` binding failures (`$n::text::jsonb`); `--` comment semicolons producing "Empty query" statements; `non_replicated_deduplication_window` moved from request-level (rejected by 25.8, server default 0 = dedup off) to table-level DDL; miscounted shipped-row fixture expectations. Also fixed `scripts/r1-colima.sh` awk word-splitting on spaced paths and the RepoDigests tag matcher.
- Task 12: complete pending live rehearsal authorization (fix rounds 1-4, 4b, parked-gap round; scoped re-reviews FAIL→PASS→PASS)
- 1 GiB rehearsal (authorized, in progress): live stack healthy; stages Preflight→Seed→Baseline→Bootstrap→InitialCheckpoint→Warmup→Quiet all PASS with three-engine digest agreement at one LSN. Nine live-only defects fixed across commits `3d9051a`..`4c7dd0e` (preflight lock self-collision; ANALYZE-bound seed loader 1446s→50s/64MiB; pg_basebackup install + conninfo; managed pg_hba for replication; ClickHouse postgresql() schema argument; checkpoint target below engine slot boundary; GrayDB missing `r1` schema registration + drained-stream proof; CH 25.8 idle-visibility sentinel; canonical digest hashing column names and name-ranked ordering — now positional values only; settle watermark for timed stages; checksums exclude services/). OPEN finding: CH CDC apply capped ~10 txns/s (serial per-commit HTTP) vs ~250 txn/s required at Cdc300 — needs a micro-batching design change (next work item). Workspace 33×0 failures after compose-contract adjustment; env remains live.
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

## Task 2 execution

- Implementer status: complete; initial commit `e5519e7`, fix commit `74e1358`.
- Focused artifact/preflight tests: PASS; task-local `cargo fmt -p graydb-r1 -- --check`: PASS.
- Initial review: FAIL for hard-coded runtime resources and Task 2 formatting drift; repo-wide clippy failures were baseline/unrelated.
- Scoped re-review: PASS; runtime resource parsing/failure behavior and task-local formatting addressed; no new in-scope regressions.

## Task 4 execution

- Implementer status: complete; initial commit `0bebe37`, fix commit `5b3d0ee`.
- Manifest/generator tests, ignored PostgreSQL integration compile, and workspace tests: PASS per implementer report; service integration remains intentionally ignored until Task 11.
- Initial review: FAIL for placeholder identity/per-table metadata and non-cumulative `analyze_ms`.
- Scoped re-review: PASS; PostgreSQL identity/size metadata and cumulative timing addressed; no new in-scope regressions.
