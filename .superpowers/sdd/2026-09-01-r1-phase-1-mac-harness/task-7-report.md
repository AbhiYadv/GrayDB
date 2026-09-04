# Task 7 implementation report

## Changes

- Added the shared `EngineAdapter`, `EngineStatus`, `QueryInvocation`, and exact-LSN `QueryResult` contract.
- Added `GrayDbAdapter` with one-time `/api/attach`, status polling, bounded timeout handling, SQL rendering, nullable string-cell decoding, elapsed-time measurement, and exact target-LSN proof validation.
- Added configurable Studio binding through `GRAYDB_STUDIO_BIND`, defaulting to `127.0.0.1`, with unit coverage for the default and explicit bind values.
- Kept Task 8/9 source files on disk but excluded their module declarations and exports from this Task 7 commit.

## Verification

- `cargo fmt -- crates/graydb-r1/src/lib.rs crates/graydb-r1/src/graydb.rs crates/graydb-r1/src/adapter.rs crates/graydb-studio/src/main.rs` — PASS.
- `cargo test -p graydb-r1 --lib graydb::tests -- --nocapture` — PASS, 2 tests.
- `cargo test -p graydb-studio --all-targets` — PASS (compile/test command exited successfully).
- Full `graydb-r1` integration test compilation is intentionally deferred because the uncommitted Task 8 ClickHouse integration test requires Task 8 module exports; those files were not staged or modified by this task.

## Post-commit cleanup

- Restored formatter spill in unrelated tracked paths under `graydb-check`, `graydb-columnar`, `graydb-ingest`, `graydb-log`, `graydb-registry`, `graydb-search`, and `graydb-studio` from commit `d9681ff`.
- Cause: the file-scoped formatting command invoked workspace formatting behavior and touched unrelated files.
- Verified `git status --short` contains only the progress ledger plus the pre-existing untracked Task 8/9 SQL, source, and integration-test files; no Task 7 file was changed.

## Review fix round 1

- Replaced adapter parsing of the human proof footer with a structured `proof_data` contract containing `target_lsn` and `visible_lsn`; the adapter requires an exact target match and visible LSN at or beyond the requested target. Human footer wording is now independent of validation.
- Refactored Studio bind parsing into pure `parse_bind_addr(Option<&str>)`; tests no longer mutate process-global environment variables, while runtime still reads `GRAYDB_STUDIO_BIND` once.
- `cargo test -p graydb-r1 --lib graydb::tests -- --nocapture` — PASS, 2 tests.
- `cargo test -p graydb-studio --all-targets` — PASS, 2 bind tests.
- Restored unrelated Studio formatter spill in `engine.rs` and `provider.rs`; only Task 7 files remain modified alongside the ledger and untracked Task 8/9 files.
