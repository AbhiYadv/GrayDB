# Task 3 implementation report

Implemented the versioned R1 schema, deterministic SplitMix64 row generator, COPY rendering, query schedule/parameters, SQL assets, and canonical result digest.

## Verification

- `cargo fmt --package graydb-r1` — passed.
- `cargo test -p graydb-r1 generator::tests -- --nocapture` — passed (2 tests).
- `cargo test -p graydb-r1 query::tests -- --nocapture` — passed (2 tests).

## Fix round 2

Normalized canonical result encoding by declared output-column identity and reordered row cells accordingly; unknown/mismatched columns remain deterministic and change the digest. Added permutation and mismatch coverage.

- `cargo fmt --package graydb-r1` — passed.
- `cargo test -p graydb-r1 query::tests -- --nocapture` — passed (4 tests).

## Fix round 3

Corrected cycle relationship allocation: each 100-row cycle now has tenant `base+1`, customers `base+1..base+6`, orders `base+1..base+21`, and events `base+1..base+61`; order/event generators select only IDs from their cycle's loaded prefixes. Added a first-cycle referential-integrity test.

- `cargo fmt --package graydb-r1` — passed.
- `cargo test -p graydb-r1 generator::tests -- --nocapture` — passed (5 tests).
- `cargo test -p graydb-r1 query::tests -- --nocapture` — passed (4 tests).
- `cargo clippy -p graydb-r1 --all-targets -- -D warnings` — blocked by pre-existing workspace lint errors in `crates/graydb-registry/src/lib.rs` (`field_reassign_with_default`) and `crates/graydb-ingest/src/snapshot.rs` (`too_many_arguments`); no Task 3 diagnostics.

## Fix round 1

Addressed review findings by adding same-tenant customer/order/event derivation, stable cycle allocation and `COPY_BATCH_ROWS = 100_000`, declared-column participation in canonical digests, and deterministic recent-weighted timestamps, long-tailed amounts, and variable structured JSON. Added relationship, prefix, allocator, and digest coverage.

- `cargo fmt --package graydb-r1` — passed.
- `cargo test -p graydb-r1 generator::tests -- --nocapture` — passed (4 tests).
- `cargo test -p graydb-r1 query::tests -- --nocapture` — passed (2 tests).

## Fix round 4

Replaced uniform modulo category selection with versioned fixed weighted
dictionaries, a bounded harmonic Zipf tenant activity rank, deterministic
long-tailed integer-cent buckets, and bounded structured metadata sizes. JSON
metadata is now rendered through `BTreeMap` values, which fixes key ordering
lexicographically at every object level. The dictionary artifact records the
distribution version, weights, Zipf formula, amount buckets, metadata limit,
and size histogram. Existing cycle-local relationships, prefix stability,
100,000-row COPY batching, and canonical query behavior remain unchanged.

New generator regressions assert observable categorical skew, bounded Zipf
ranks, metadata size diversity/bound, and an exact serialized JSON value whose
key order fails if map ordering regresses.

- `cargo fmt --package graydb-r1` — passed.
- `cargo test -p graydb-r1 generator::tests -- --nocapture` — passed (8 tests).
- `cargo test -p graydb-r1 query::tests -- --nocapture` — passed (4 tests).
- `cargo test -p graydb-r1 -- --nocapture` — passed (19 unit tests; 0 doc tests).

## Fix round 5

Moved bounded Zipf tenant activity from tenant metadata into actual generated
ownership. Each customer deterministically selects a tenant rank from the
already-loaded tenant-cycle prefix, capped at 64 ranks; orders inherit the
selected customer's tenant and events inherit the selected order's tenant.
Customer and order references remain cycle-local, so every relationship exists
before it is referenced and generation remains prefix-stable. Updated the
distribution artifact to version 3 with the ownership rule.

Added regressions that measure materially skewed customer, order, and event
ownership across 512 complete cycles and verify every referenced tenant is
present in the progressively loaded tenant-cycle prefix. The ownership
regression failed against the round-4 implementation with equal hot/cold
customer counts (`hot=5, cold=5`) before passing with the fix.

- `cargo fmt --package graydb-r1` — passed.
- `cargo test -p graydb-r1 generator::tests -- --nocapture` — passed (10 tests).
- `cargo test -p graydb-r1 query::tests -- --nocapture` — passed (4 tests).
- `cargo test -p graydb-r1 -- --nocapture` — passed (21 unit tests; 0 doc tests).
