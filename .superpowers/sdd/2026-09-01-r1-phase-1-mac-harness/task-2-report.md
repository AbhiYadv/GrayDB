# Task 2 Report

Plan: `docs/superpowers/plans/2026-09-01-r1-phase-1-mac-harness.md`
Brief: `task-2-brief.md`
Commit: `e5519e7` (`feat(r1): add append-only artifacts and preflight gates`)

## Fix Round 1

- Replaced the hard-coded system resource snapshot with real values parsed from `colima status --profile r1 --json` and `docker info --format '{{json .}}'`.
- Made `SystemPreflightProbe` return a failure when either runtime command exits nonzero.
- Added focused tests for runtime resource extraction and for the undersized / failed-runtime path.
- Ran `cargo fmt -p graydb-r1 -- --check` successfully after formatting the touched crate files.

## Implemented

- Added `crates/graydb-r1/src/artifacts.rs` with:
  - `RunDirectory::create(root, run_id)`
  - `EventSink::emit(event)`
  - append-only `run.log` and `events.jsonl`
  - `Event`, `EventLevel`, and redacted rendering
  - `sha256_tree(root)` with sorted relative paths and checksum output
- Added `crates/graydb-r1/src/preflight.rs` with:
  - `PreflightSnapshot`
  - `PreflightPolicy::r1_mac().evaluate(&snapshot)`
  - `PreflightReport` / `PreflightFailure`
  - `PreflightProbe`
  - `SnapshotPreflightProbe`
  - `SystemPreflightProbe`
- Wired the new modules through `crates/graydb-r1/src/lib.rs`.
- Added `.r1/`, `.env.r1`, and `bench-results/r1-p1-v1-*/` to `.gitignore`.

## Verification

- `cargo test -p graydb-r1 artifacts::tests -- --nocapture`
  - PASS
  - 2 tests passed: redaction and checksum-tree coverage.
- `cargo test -p graydb-r1 preflight::tests -- --nocapture`
  - PASS
  - 4 tests passed: projected-space rejection, happy-path policy, runtime resource extraction, and failed-runtime handling.
- `cargo fmt -p graydb-r1 -- --check`
  - PASS
- `cargo clippy -p graydb-r1 --all-targets -- -D warnings`
  - FAIL
  - Stopped in unrelated `crates/graydb-registry/src/lib.rs:138` with `clippy::field-reassign-with-default`, then in unrelated `crates/graydb-ingest/src/snapshot.rs:282` with `clippy::too-many-arguments`.

## Notes

- The initial filtered baseline commands did not hard-fail; they compiled the crate and returned zero matching tests before Task 2 modules existed.
- Task 2 code changes are committed. The remaining clippy failures are workspace-wide blockers outside the Task 2 crate.
