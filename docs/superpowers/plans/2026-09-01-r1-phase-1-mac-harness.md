# R1 Phase 1 Mac Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the deterministic, visible, exact-LSN Mac Phase 0 harness that generates the R1 dataset, drives PostgreSQL CDC into GrayDB and ClickHouse, invalidates incorrect runs, and reports per-operation and total execution time.

**Architecture:** Add a focused `graydb-r1` Rust crate for benchmark contracts, generation, workload replay, adapters, validation, metrics, and orchestration. Reuse GrayDB Studio as the independently killable GrayDB service, reuse the existing raw replication client and incremental decoder for ClickHouse CDC, and control PostgreSQL, GrayDB, and ClickHouse through a dedicated Docker Compose project on the Crucial X9 Colima profile. Run the controller, deterministic writer, query driver, and validator as one release-built host process so its terminal output remains directly visible. Preserve the old `bench-cdc` binary as historical evidence.

**Tech Stack:** Rust 1.95, Tokio, tokio-postgres, existing GrayDB crates, Axum/HTTP, ClickHouse HTTP `JSONEachRow`, Docker Compose 28.4, Colima 0.10.3, PostgreSQL 17, ClickHouse 25.8 LTS, TOML configuration, JSON/JSONL artifacts, SHA-256 checksums.

**Spec:** `docs/superpowers/specs/2026-09-01-r1-phase-1-benchmark-design.md`

## Global Constraints

- Benchmark identity is `R1-P1-v1`; deterministic seed is `20260901`.
- Mac scales are 1, 10, 50, 100, and optional 200 GiB of measured PostgreSQL published-table bytes.
- The official 1 TiB conclusion remains AWS-only; this plan must not create AWS resources.
- The application mix is 90% inserts, 8% updates, and 2% deletes by affected rows.
- Correctness mode compares one numeric source LSN; isolated mode compares one logical checkpoint through a replay-specific LSN map.
- One gap, duplicate, stale row, tombstone error, or result mismatch invalidates all performance numbers in that run.
- The Mac Colima profile uses 8 virtual CPUs, 12 GiB memory, and a 600 GiB disk image on `/Volumes/Crucial X9/GrayDB/.r1`.
- Preflight requires 20% projected free space; runtime stops writes at 15% free space.
- All benchmark commands stream readable logs to the terminal and append structured events to the run directory.
- Secrets are accepted only through environment variables and are redacted from terminal and file artifacts.
- Release builds are mandatory for measured numbers. Debug builds must label results invalid for performance comparison.
- Kubernetes and Minikube remain outside Phase 0.
- `crates/graydb-check/src/bin/bench-cdc.rs` and existing `bench-results/r1-local-*` files remain unchanged.
- Each task stages only its listed files and ends with the listed focused commit.

## File and module map

| Path | Responsibility |
| --- | --- |
| `crates/graydb-r1/src/contracts.rs` | benchmark IDs, profiles, run modes, checkpoints, timing rules |
| `crates/graydb-r1/src/artifacts.rs` | run directory, redacted terminal log, JSONL events, checksums, lock |
| `crates/graydb-r1/src/preflight.rs` | external-volume, free-space, Colima, Docker, CPU, and memory gates |
| `crates/graydb-r1/src/generator.rs` | deterministic rows, COPY batches, size-driven loading |
| `crates/graydb-r1/src/manifest.rs` | dataset, workload, environment, replay, and result manifests |
| `crates/graydb-r1/src/workload.rs` | exact transaction planning, rate control, deterministic clock |
| `crates/graydb-r1/src/ledger.rs` | intent log, committed transaction ledger, resume verification |
| `crates/graydb-r1/src/replication.rs` | pgoutput transaction assembly and sequence-to-LSN mapping |
| `crates/graydb-r1/src/adapter.rs` | common engine interface and shared query result types |
| `crates/graydb-r1/src/graydb.rs` | GrayDB Studio HTTP adapter |
| `crates/graydb-r1/src/clickhouse.rs` | ClickHouse DDL, initial load, CDC sink, exact-LSN query adapter |
| `crates/graydb-r1/src/query.rs` | Q1-Q5 parameters, deterministic schedule, canonical result hashing |
| `crates/graydb-r1/src/oracle.rs` | ledger-state and PostgreSQL checkpoint correctness proofs |
| `crates/graydb-r1/src/metrics.rs` | latency histograms, resource samples, stage timing |
| `crates/graydb-r1/src/verdict.rs` | invalidation, cell comparison, and overall winner rules |
| `crates/graydb-r1/src/failure.rs` | controlled engine, connection, writer, and controller interruptions |
| `crates/graydb-r1/src/controller.rs` | resumable stage state machine and service orchestration |
| `crates/graydb-r1/src/report.rs` | `result.json`, `result.md`, storage estimate, AWS capacity request |
| `crates/graydb-r1/src/bin/r1ctl.rs` | operator-facing CLI |
| `bench/r1/` | versioned SQL, dictionaries, profile configuration, Compose, Dockerfile |
| `scripts/r1-colima.sh` | safe creation and inspection of the dedicated external-disk profile |

---

### Task 1: Create the benchmark crate and freeze profile contracts

**Files:**
- Modify: `Cargo.toml:2-18`
- Modify: `Cargo.lock`
- Create: `crates/graydb-r1/Cargo.toml`
- Create: `crates/graydb-r1/src/lib.rs`
- Create: `crates/graydb-r1/src/contracts.rs`
- Create: `bench/r1/profiles.toml`

**Interfaces:**
- Produces: `ScaleProfile`, `ProfileSpec`, `RunMode`, `EngineKind`, `LogicalCheckpoint`, `RunConfig`, and `ProfileCatalog::load(path)`.
- Consumes: no R1-specific interface.

- [ ] **Step 1: Add a failing profile-contract test**

Add a test in `contracts.rs` that loads `bench/r1/profiles.toml` and asserts every frozen value:

```rust
#[test]
fn profile_catalog_matches_r1_p1_v1() {
    let catalog = ProfileCatalog::load(repo_file("bench/r1/profiles.toml")).unwrap();
    let smoke = catalog.get(ScaleProfile::MacSmoke).unwrap();
    assert_eq!(smoke.minimum_bytes, 1_u64 << 30);
    assert_eq!(smoke.repetitions, 1);
    assert_eq!(smoke.warmup_secs, 60);
    assert_eq!(smoke.quiet_secs, 120);
    assert_eq!(smoke.fixed_rate_secs, 120);
    assert_eq!(smoke.search_step_secs, 120);
    assert_eq!(smoke.maximum_rate, 2_000);

    let aws = catalog.get(ScaleProfile::AwsPhase1).unwrap();
    assert_eq!(aws.minimum_bytes, 1_u64 << 40);
    assert_eq!(aws.repetitions, 3);
    assert_eq!(aws.warmup_secs, 900);
    assert_eq!(aws.quiet_secs, 1_800);
    assert_eq!(aws.fixed_rate_secs, 1_800);
    assert_eq!(aws.search_step_secs, 900);
    assert_eq!(aws.maximum_rate, 64_000);
}
```

- [ ] **Step 2: Run the test and confirm the missing crate fails**

Run: `cargo test -p graydb-r1 contracts::tests::profile_catalog_matches_r1_p1_v1 -- --exact`

Expected: FAIL because package `graydb-r1` does not exist.

- [ ] **Step 3: Add the crate, dependencies, and frozen TOML**

Add `crates/graydb-r1` to workspace members. Add workspace dependencies `clap = { version = "4", features = ["derive"] }`, `fs2 = "0.4"`, `reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }`, `hdrhistogram = "7"`, and `serde_yaml = "0.9"`. Add `tempfile = "3"` and `wiremock = "0.6"` under the new crate's development dependencies. Its normal dependencies are `anyhow`, `async-trait`, `bytes`, `clap`, `fs2`, `futures-util`, `graydb-columnar`, `graydb-ingest`, `graydb-log`, `graydb-registry`, `graydb-studio`, `hdrhistogram`, `reqwest`, `serde`, `serde_json`, `serde_yaml`, `sha2`, `tokio`, `tokio-postgres`, `toml`, `tracing`, and `tracing-subscriber` from the workspace.

Define this test helper in the `contracts.rs` test module:

```rust
fn repo_file(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join(relative)
}
```

Define the contracts exactly:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum ScaleProfile { MacSmoke, MacCorrectness, MacValidation, MacStress, MacCeiling, AwsPhase1 }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum RunMode { Correctness, Isolated }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum EngineKind { Graydb, Clickhouse }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalCheckpoint { pub sequence: u64, pub source_lsn: u64 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileSpec {
    pub minimum_bytes: u64,
    pub repetitions: u8,
    pub warmup_secs: u64,
    pub quiet_secs: u64,
    pub fixed_rate_secs: u64,
    pub search_step_secs: u64,
    pub maximum_rate: u64,
}
```

The TOML must encode all six timing rows from spec section 13, fixed rates `[300, 1000]`, search rates `[2000, 4000, 8000, 16000, 32000, 64000]`, freshness p99 limit `1000`, backlog limit `10737418240`, minimum query samples `30`, seed `20260901`, and benchmark ID `R1-P1-v1`.

- [ ] **Step 4: Run focused and workspace tests**

Run: `cargo test -p graydb-r1 contracts::tests -- --nocapture`

Expected: PASS.

Run: `cargo test --workspace`

Expected: all existing and new tests PASS.

- [ ] **Step 5: Commit the contract**

```bash
git add Cargo.toml Cargo.lock crates/graydb-r1 bench/r1/profiles.toml
git commit -m "feat(r1): freeze benchmark profile contracts"
```

---

### Task 2: Build append-only artifacts and storage preflight

**Files:**
- Modify: `.gitignore`
- Modify: `crates/graydb-r1/src/lib.rs`
- Create: `crates/graydb-r1/src/artifacts.rs`
- Create: `crates/graydb-r1/src/preflight.rs`

**Interfaces:**
- Consumes: `RunConfig`, `ScaleProfile`, `ProfileSpec` from Task 1.
- Produces: `RunDirectory::create(root, run_id)`, `EventSink::emit(event)`, `PreflightProbe`, `PreflightPolicy::evaluate(snapshot)`, `PreflightReport`, and `sha256_tree(root)`.

- [ ] **Step 1: Write failing artifact and policy tests**

```rust
#[test]
fn redacts_credentials_from_both_log_formats() {
    let event = Event::info("connect", "postgres://postgres:hunter2@pg/appdb")
        .with_secret("hunter2");
    let rendered = event.render_redacted();
    assert!(!rendered.human.contains("hunter2"));
    assert!(!rendered.json.contains("hunter2"));
    assert!(rendered.human.contains("[REDACTED]"));
}

#[test]
fn rejects_projected_space_below_twenty_percent() {
    let snapshot = PreflightSnapshot {
        volume_bytes: 1_000,
        available_bytes: 500,
        expected_peak_bytes: 350,
        runtime_stop_bytes: 150,
        cpus: 10,
        memory_bytes: 16_u64 << 30,
        data_path_on_expected_volume: true,
        colima_disk_bytes: 600_u64 << 30,
        lock_available: true,
    };
    let report = PreflightPolicy::r1_mac().evaluate(&snapshot);
    assert!(!report.passed);
    assert_eq!(report.failures[0].code, "PROJECTED_FREE_BELOW_20_PERCENT");
}
```

- [ ] **Step 2: Verify the tests fail**

Run: `cargo test -p graydb-r1 artifacts::tests -- --nocapture`

Expected: FAIL because the artifacts module is absent.

Run: `cargo test -p graydb-r1 preflight::tests -- --nocapture`

Expected: FAIL because the preflight module is absent.

- [ ] **Step 3: Implement the run directory and append-only sink**

Use `fs2::FileExt::try_lock_exclusive` on `<run>/run.lock`. Create exactly the artifact tree from spec section 17. Open `run.log` and `events.jsonl` with `create(true).append(true)`. Emit this serializable event shape:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub monotonic_ns: u128,
    pub wall_unix_ms: u128,
    pub level: EventLevel,
    pub stage: String,
    pub operation: String,
    pub message: String,
    pub fields: BTreeMap<String, serde_json::Value>,
}
```

Flush both outputs after every stage boundary, correctness verdict, and failure event. `sha256_tree` must sort relative paths, exclude `run.lock` and `SHA256SUMS`, hash file contents, then write GNU-compatible checksum lines.

- [ ] **Step 4: Implement real and fake preflight probes**

`SystemPreflightProbe` must canonicalize the data root, verify it begins with `/Volumes/Crucial X9/GrayDB/.r1`, use `fs2::total_space` and `fs2::available_space`, run a 64 MiB write-plus-`sync_all` probe inside the run root, inspect `colima status --profile r1 --json`, and inspect `docker info --format '{{json .}}'`. Record every raw probe result in `environment.json`; do not record environment values whose keys contain `PASSWORD`, `TOKEN`, `SECRET`, or `KEY`.

Add `.r1/`, `.env.r1`, and `bench-results/r1-p1-v1-*/` to `.gitignore`.

- [ ] **Step 5: Run tests and static checks**

Run: `cargo test -p graydb-r1 artifacts::tests -- --nocapture`

Expected: PASS.

Run: `cargo test -p graydb-r1 preflight::tests -- --nocapture`

Expected: PASS.

Run: `cargo fmt --all -- --check`

Expected: PASS.

Run: `cargo clippy -p graydb-r1 --all-targets -- -D warnings`

Expected: PASS with no warnings.

- [ ] **Step 6: Commit artifact safety**

```bash
git add .gitignore Cargo.lock crates/graydb-r1
git commit -m "feat(r1): add append-only artifacts and preflight gates"
```

---

### Task 3: Add the versioned schema, queries, and deterministic row generator

**Files:**
- Modify: `crates/graydb-r1/src/lib.rs`
- Create: `crates/graydb-r1/src/generator.rs`
- Create: `crates/graydb-r1/src/query.rs`
- Create: `bench/r1/schema.sql`
- Create: `bench/r1/dictionaries.json`
- Create: `bench/r1/queries/q1.sql`
- Create: `bench/r1/queries/q2.sql`
- Create: `bench/r1/queries/q3.sql`
- Create: `bench/r1/queries/q4.sql`
- Create: `bench/r1/queries/q5.sql`

**Interfaces:**
- Consumes: benchmark seed and `LogicalCheckpoint` from Task 1.
- Produces: `DeterministicGenerator::row(table, id)`, `copy_batch(table, range)`, `QueryId`, `QueryParameters::for_checkpoint`, `QuerySchedule::new(seed)`, and `canonical_digest(result)`.

- [ ] **Step 1: Write failing determinism and query tests**

```rust
#[test]
fn same_seed_and_range_produce_identical_copy_bytes() {
    let a = DeterministicGenerator::new(20260901).copy_batch(Table::Orders, 1..10_001).unwrap();
    let b = DeterministicGenerator::new(20260901).copy_batch(Table::Orders, 1..10_001).unwrap();
    assert_eq!(a.bytes, b.bytes);
    assert_eq!(a.sha256, b.sha256);
    assert_eq!(a.rows, 10_000);
}

#[test]
fn schedule_and_parameters_ignore_wall_clock() {
    let c = LogicalCheckpoint { sequence: 42_000, source_lsn: 0xA000_1234 };
    let a = QuerySchedule::new(20260901).at(17, c);
    let b = QuerySchedule::new(20260901).at(17, c);
    assert_eq!(a, b);
    assert_eq!(a.parameters, QueryParameters::for_checkpoint(20260901, c));
}
```

- [ ] **Step 2: Verify the tests fail**

Run: `cargo test -p graydb-r1 generator::tests -- --nocapture`

Expected: FAIL because the generator module is absent.

Run: `cargo test -p graydb-r1 query::tests -- --nocapture`

Expected: FAIL because the query module is absent.

- [ ] **Step 3: Add the exact PostgreSQL schema**

`schema.sql` must create `r1.tenants`, `r1.customers`, `r1.orders`, and `r1.order_events` with the columns and types from spec section 4. It must also create `r1_control.tx_marker(sequence bigint primary key, operation_sha256 text not null, committed_at timestamptz not null default clock_timestamp())`, publication `graydb_r1_pub` for the four data tables, and publication `graydb_r1_control_pub` for `tx_marker`. Every data column is `NOT NULL`; each table has its specified bigint primary key; no foreign key or cascade is allowed.

- [ ] **Step 4: Implement a version-stable generator**

Do not use library RNG behavior. Implement SplitMix64 directly so dependency updates cannot change data:

```rust
fn mix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn draw(seed: u64, table: Table, row_id: u64, salt: u64) -> u64 {
    mix64(seed ^ table.tag() ^ row_id.rotate_left(17) ^ salt.rotate_left(41))
}
```

Render COPY text through `graydb-columnar::copytext`-compatible escaping. JSON objects must use `BTreeMap` key order. Batch size is 100,000 rows. Allocate row IDs in table-ratio cycles so every smaller profile is a prefix of every larger profile.

- [ ] **Step 5: Add exact Q1-Q5 assets and canonicalization**

Copy the logical SQL from spec section 11 into the five files. Replace named parameters only through `QueryParameters`; reject unresolved `:` parameters before execution. Canonicalization must sort columns by declared query output order, encode null as `N;`, encode a value as `<byte-length>:<UTF-8>;`, sort rows lexicographically, and SHA-256 the concatenation.

- [ ] **Step 6: Run focused tests**

Run: `cargo test -p graydb-r1 generator::tests -- --nocapture`

Expected: PASS.

Run: `cargo test -p graydb-r1 query::tests -- --nocapture`

Expected: PASS, including tests that row order does not affect the digest and value boundaries do affect it.

- [ ] **Step 7: Commit schema and generation**

```bash
git add crates/graydb-r1 bench/r1/schema.sql bench/r1/dictionaries.json bench/r1/queries
git commit -m "feat(r1): add deterministic dataset and query suite"
```

---

### Task 4: Load to measured bytes and write immutable dataset manifests

**Files:**
- Modify: `crates/graydb-r1/src/lib.rs`
- Modify: `crates/graydb-r1/src/generator.rs`
- Create: `crates/graydb-r1/src/manifest.rs`
- Create: `crates/graydb-r1/tests/postgres_dataset.rs`

**Interfaces:**
- Consumes: `ProfileSpec`, `DeterministicGenerator`, `RunDirectory`, `EventSink`.
- Produces: `DatasetLoader::load_until(minimum_bytes) -> DatasetManifest`, `PublishedSizeProbe`, and `DatasetManifest::content_hash()`.

- [ ] **Step 1: Write failing size and manifest tests**

```rust
#[tokio::test]
async fn loader_stops_only_after_measured_threshold() {
    let probe = FakeSizeProbe::new([400, 800, 1_200]);
    let loader = DatasetLoader::with_probe(probe, FakeCopySink::default(), 100);
    let manifest = loader.load_until(1_000).await.unwrap();
    assert_eq!(manifest.published_table_bytes, 1_200);
    assert_eq!(manifest.batches.len(), 3);
}

#[test]
fn content_hash_excludes_run_timestamps() {
    let mut a = fixture_manifest();
    let mut b = a.clone();
    b.load_started_unix_ms += 50_000;
    b.load_finished_unix_ms += 50_000;
    assert_eq!(a.content_hash().unwrap(), b.content_hash().unwrap());
}
```

In the local test module, `FakeSizeProbe` owns a `VecDeque<u64>` and returns one value from `published_size()` after each batch; `FakeCopySink` records accepted `CopyBatch` values without PostgreSQL; `fixture_manifest()` returns a complete two-table manifest with fixed hashes and timestamps.

- [ ] **Step 2: Verify the tests fail**

Run: `cargo test -p graydb-r1 generator::tests::loader_stops_only_after_measured_threshold -- --exact`

Expected: FAIL because measured loading is absent.

Run: `cargo test -p graydb-r1 manifest::tests::content_hash_excludes_run_timestamps -- --exact`

Expected: FAIL because manifest contracts are absent.

- [ ] **Step 3: Implement measured loading**

Use `tokio_postgres::Client::copy_in` for each 100,000-row batch. After a complete table-ratio cycle, run `ANALYZE` and the exact `pg_table_size` publication query from spec section 4.1. Continue until `published_table_bytes >= minimum_bytes`. Record per-table rows and sizes, index bytes, total relation bytes, schema and dictionary hashes, PostgreSQL version and settings, batch hashes, generation time, COPY time, ANALYZE time, and initial LSN.

- [ ] **Step 4: Implement immutable hashing and atomic manifest writes**

Serialize hashable content into a separate `DatasetIdentity` with no timestamps, host paths, or run ID. Write JSON to `dataset-manifest.json.partial`, call `sync_all`, rename to `dataset-manifest.json`, then sync the parent directory. Refuse to overwrite an existing final manifest whose hash differs.

- [ ] **Step 5: Add a real PostgreSQL integration test**

Mark `postgres_dataset.rs` with `#[ignore = "requires the r1 PostgreSQL service"]`. Load 64 MiB twice into fresh schemas, assert both logical content hashes match, assert each measured size meets the threshold, and assert all four row-count queries match.

Run: `cargo test -p graydb-r1 --test postgres_dataset --no-run`

Expected: the ignored integration target compiles; Task 11 executes it against the service environment.

- [ ] **Step 6: Run unit and workspace tests**

Run: `cargo test -p graydb-r1 manifest::tests -- --nocapture`

Expected: PASS.

Run: `cargo test -p graydb-r1 generator::tests -- --nocapture`

Expected: PASS.

Run: `cargo test --workspace`

Expected: PASS; ignored service tests are listed but not run.

- [ ] **Step 7: Commit dataset loading**

```bash
git add crates/graydb-r1
git commit -m "feat(r1): load datasets to measured PostgreSQL bytes"
```

---

### Task 5: Build deterministic transactions, the intent log, and committed ledger

**Files:**
- Modify: `crates/graydb-r1/src/lib.rs`
- Create: `crates/graydb-r1/src/workload.rs`
- Create: `crates/graydb-r1/src/ledger.rs`

**Interfaces:**
- Consumes: seed, `LogicalCheckpoint`, `EventSink`.
- Produces: `Operation`, `TransactionPlan`, `WorkloadPlanner::plan(sequence)`, `RateLimiter`, `IntentLog::append`, `CommittedLedger::append`, `CommittedLedger::resume`, and `LedgerEntry`.

- [ ] **Step 1: Write failing workload distribution and recovery tests**

```rust
#[test]
fn planner_is_reproducible_and_converges_on_row_mix() {
    let planner = WorkloadPlanner::new(20260901);
    let a: Vec<_> = (1..=20_000).map(|s| planner.plan(s)).collect();
    let b: Vec<_> = (1..=20_000).map(|s| planner.plan(s)).collect();
    assert_eq!(a, b);
    let mix = RowMix::from_plans(&a);
    assert!((mix.insert_fraction() - 0.90).abs() <= 0.005);
    assert!((mix.update_fraction() - 0.08).abs() <= 0.005);
    assert!((mix.delete_fraction() - 0.02).abs() <= 0.005);
}

#[test]
fn resume_rejects_gap_duplicate_and_bad_checksum() {
    let dir = tempfile::tempdir().unwrap();
    let mut ledger = CommittedLedger::create(dir.path()).unwrap();
    ledger.append(fixture_entry(1)).unwrap();
    ledger.append(fixture_entry(2)).unwrap();
    assert_eq!(CommittedLedger::resume(dir.path()).unwrap().next_sequence(), 3);
    corrupt_second_line(&dir.path().join("workload-ledger.jsonl"));
    assert!(CommittedLedger::resume(dir.path()).is_err());
}
```

Define `marker_transaction_frames(sequence, hash, end_lsn)` beside the test. It must construct contiguous `graydb_log::Frame` values using the same wire encodings as `graydb-registry/src/decoder.rs` tests, with relation name `r1_control.tx_marker`, xid `9001`, the supplied marker values, and `txn_complete = true` only on Commit.

Define `fixture_entry(sequence)` in the same test module with `source_lsn = 100 + sequence`, a fixed operation hash derived from the sequence, and the correct previous-entry hash. Define `corrupt_second_line(path)` to replace one byte in the second JSONL record without changing the line count.

- [ ] **Step 2: Verify the tests fail**

Run: `cargo test -p graydb-r1 workload::tests -- --nocapture`

Expected: FAIL because the workload module is absent.

Run: `cargo test -p graydb-r1 ledger::tests -- --nocapture`

Expected: FAIL because the ledger module is absent.

- [ ] **Step 3: Implement the exact transaction planner**

Define operations with complete values so replay performs no fresh random choices:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    InsertCustomer(CustomerRow),
    InsertOrder(OrderRow),
    InsertOrderEvent(OrderEventRow),
    UpdateCustomer { customer_id: u64, segment: String, profile_json: String },
    UpdateOrder { order_id: u64, status: String, amount_cents: i64, updated_at_micros: i64 },
    DeleteOrder { order_id: u64 },
    DeleteOrderEvent { event_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionPlan {
    pub sequence: u64,
    pub logical_time_micros: i64,
    pub operations: Vec<Operation>,
    pub operation_sha256: String,
}
```

Use the task-3 `draw` function. Choose transaction sizes with thresholds 9500, 9900, 9990, and 10000 over `draw % 10000`. Enforce the table ratios from spec section 6 across affected rows through deterministic weighted round-robin counters stored in the planner state.

- [ ] **Step 4: Implement durable intent and commit records**

Before sending SQL, append the full `TransactionPlan` to `workload-intents.jsonl` and `sync_data`. After the control decoder observes its commit, append:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub sequence: u64,
    pub xid: u32,
    pub source_lsn: u64,
    pub operation_sha256: String,
    pub committed_unix_ms: u128,
    pub previous_entry_sha256: String,
    pub entry_sha256: String,
}
```

Recompute and verify the hash chain on resume. A valid intent without a committed ledger entry is `UnknownCommit`; resolve it through `r1_control.tx_marker` before retrying. Never repeat a sequence found in the control table.

- [ ] **Step 5: Implement monotonic rate limiting**

`RateLimiter::acquire(rows)` uses `tokio::time::Instant`, accumulates fractional row tokens, caps burst capacity at the largest transaction for the profile, and records target and achieved rows per one-minute interval. Add a paused clock test using Tokio time control.

- [ ] **Step 6: Run tests and commit**

Run: `cargo test -p graydb-r1 workload::tests -- --nocapture`

Expected: PASS.

Run: `cargo test -p graydb-r1 ledger::tests -- --nocapture`

Expected: PASS, including deterministic resume and unknown-commit tests.

```bash
git add crates/graydb-r1
git commit -m "feat(r1): add deterministic workload ledger"
```

---

### Task 6: Map committed transactions to source LSNs and replay checkpoints

**Files:**
- Modify: `crates/graydb-r1/src/lib.rs`
- Create: `crates/graydb-r1/src/replication.rs`
- Create: `crates/graydb-r1/tests/postgres_workload.rs`

**Interfaces:**
- Consumes: `ReplClient`, `ReplMsg`, `StreamDecoder`, `TransactionPlan`, `CommittedLedger`.
- Produces: `ControlLsnMapper::feed(frame) -> Option<LedgerCommit>`, `ApplicationWriter::run(target_rate, stop)`, `ReplayMap`, and `WorkloadReplayer::replay(entries)`.

- [ ] **Step 1: Write failing transaction-to-LSN tests**

Construct pgoutput frames for Relation, Begin, marker Insert, application Insert, and Commit. Assert no mapping appears before Commit and the final mapping uses Commit `end_lsn`:

```rust
#[test]
fn marker_becomes_visible_only_at_commit_end_lsn() {
    let mut mapper = ControlLsnMapper::new();
    for frame in marker_transaction_frames(77, "abc", 0xA000_0042) {
        let mapped = mapper.feed(frame).unwrap();
        if frame.txn_complete {
            assert_eq!(mapped.unwrap(), LedgerCommit {
                sequence: 77,
                xid: 9001,
                source_lsn: 0xA000_0042,
                operation_sha256: "abc".into(),
            });
        } else {
            assert!(mapped.is_none());
        }
    }
}
```

- [ ] **Step 2: Verify the test fails**

Run: `cargo test -p graydb-r1 replication::tests::marker_becomes_visible_only_at_commit_end_lsn -- --exact`

Expected: FAIL because `ControlLsnMapper` is absent.

- [ ] **Step 3: Implement the control replication loop**

Create slot `graydb_r1_control_slot`, start pgoutput from the run's initial LSN, and write every raw frame to a dedicated `graydb-log::FrameLog` before decoding. Feed durable frames to `StreamDecoder`; select changes for `r1_control.tx_marker`; emit `LedgerCommit` only when `DecodedBatch.last_commit_lsn` is nonzero. Acknowledge only the control frame log's durable commit mark.

- [ ] **Step 4: Implement SQL transaction execution**

For each `TransactionPlan`, open one PostgreSQL transaction, execute all operations with bound parameters, insert `r1_control.tx_marker`, query `txid_current()`, and commit. The ledger writer waits for the matching `LedgerCommit` before declaring success. If the SQL connection drops after commit submission, use the control table and mapper to classify the intent before any retry.

- [ ] **Step 5: Implement replay-specific LSN maps**

Replay committed intent plans in sequence order against a restored source. Write `replay-map.jsonl` entries containing `logical_sequence`, original source LSN, replay source LSN, and operation hash. Refuse a mismatched operation hash or non-contiguous sequence.

- [ ] **Step 6: Add and run PostgreSQL integration coverage**

The ignored `postgres_workload.rs` test must commit 100 mixed transactions, kill the writer after one SQL commit but before ledger append, resume, and prove 100 unique marker sequences, a contiguous ledger, and identical row digests.

Run: `cargo test -p graydb-r1 --test postgres_workload --no-run`

Expected: the ignored integration target compiles; Task 11 executes it against the service environment.

Run: `cargo test -p graydb-r1 replication::tests`

Expected: PASS.

Run: `cargo test -p graydb-r1 workload::tests`

Expected: PASS.

Run: `cargo test -p graydb-r1 ledger::tests`

Expected: PASS.

- [ ] **Step 7: Commit source-LSN replay**

```bash
git add crates/graydb-r1
git commit -m "feat(r1): map workload commits to source LSNs"
```

Define `invocation_with_lsn(lsn)` in the same test module as a Q2 `QueryInvocation` with seed-derived parameters, logical sequence 17, and the supplied target and visible LSN expectation.

---

### Task 7: Define the engine contract and GrayDB adapter

**Files:**
- Modify: `crates/graydb-r1/src/lib.rs`
- Modify: `crates/graydb-studio/src/main.rs:20-52`
- Create: `crates/graydb-r1/src/adapter.rs`
- Create: `crates/graydb-r1/src/graydb.rs`

**Interfaces:**
- Consumes: `QueryId`, `QueryParameters`, `LogicalCheckpoint`.
- Produces: `EngineAdapter`, `EngineStatus`, `QueryInvocation`, `QueryResult`, `GrayDbAdapter`, and a configurable Studio bind address.

- [ ] **Step 1: Write failing adapter contract tests**

```rust
#[tokio::test]
async fn graydb_query_requires_matching_lsn_proof() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/api/query"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(
            json!({"columns":["status","count"],"rows":[["paid","2"]],"proof":"LSN A/42"})
        ))
        .mount(&server)
        .await;
    let adapter = GrayDbAdapter::new(server.uri());
    let err = adapter.query(invocation_with_lsn(0xA000_0043)).await.unwrap_err();
    assert!(err.to_string().contains("LSN proof mismatch"));
}
```

- [ ] **Step 2: Verify the test fails**

Run: `cargo test -p graydb-r1 graydb::tests::graydb_query_requires_matching_lsn_proof -- --exact`

Expected: FAIL because the adapter is absent.

- [ ] **Step 3: Add the common engine interface**

```rust
#[async_trait]
pub trait EngineAdapter: Send + Sync {
    fn kind(&self) -> EngineKind;
    async fn status(&self) -> Result<EngineStatus>;
    async fn wait_visible(&self, target_lsn: u64, timeout: Duration) -> Result<Duration>;
    async fn query(&self, invocation: &QueryInvocation) -> Result<QueryResult>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub target_lsn: u64,
    pub visible_lsn: u64,
    pub elapsed_ns: u128,
    pub rows_read: Option<u64>,
    pub bytes_read: Option<u64>,
}
```

- [ ] **Step 4: Implement GrayDB HTTP behavior**

POST `/api/attach` once, poll `/api/status`, and POST `/api/query` with `class: "target_lsn=<formatted LSN>"`. Parse and require the returned proof to equal the requested LSN. Convert Studio strings into nullable result cells without lossy numeric parsing.

Modify Studio to read `GRAYDB_STUDIO_BIND`, default `127.0.0.1`, and bind `0.0.0.0` only in the benchmark Compose service. Add a unit test for default and explicit bind parsing.

- [ ] **Step 5: Run tests and commit**

Run: `cargo test -p graydb-r1 adapter::tests -- --nocapture`

Expected: PASS.

Run: `cargo test -p graydb-r1 graydb::tests -- --nocapture`

Expected: PASS.

Run: `cargo test -p graydb-studio --all-targets`

Expected: PASS.

```bash
git add crates/graydb-r1 crates/graydb-studio/src/main.rs
git commit -m "feat(r1): add exact-LSN GrayDB benchmark adapter"
```

---

### Task 8: Build the ClickHouse versioned CDC sink and exact query adapter

**Files:**
- Modify: `crates/graydb-r1/src/lib.rs`
- Create: `crates/graydb-r1/src/clickhouse.rs`
- Create: `bench/r1/clickhouse.sql`
- Create: `bench/r1/queries/clickhouse/q1.sql`
- Create: `bench/r1/queries/clickhouse/q2.sql`
- Create: `bench/r1/queries/clickhouse/q3.sql`
- Create: `bench/r1/queries/clickhouse/q4.sql`
- Create: `bench/r1/queries/clickhouse/q5.sql`
- Create: `crates/graydb-r1/tests/clickhouse_cdc.rs`

**Interfaces:**
- Consumes: `ReplClient`, `StreamDecoder`, `TypedChange`, `EngineAdapter`, `QueryInvocation`.
- Produces: `Version::from_lsn_ordinal`, `ClickHouseSink::apply`, `ClickHouseAdapter`, and `ClickHouseStatus`.

- [ ] **Step 1: Write failing version and tombstone tests**

```rust
#[test]
fn version_orders_changes_inside_and_across_commits() {
    let a = Version::from_lsn_ordinal(100, 1);
    let b = Version::from_lsn_ordinal(100, 2);
    let c = Version::from_lsn_ordinal(101, 0);
    assert!(a < b && b < c);
    assert_eq!(c.as_u128(), (101_u128 << 32));
}

#[test]
fn visible_version_excludes_tombstone_at_target_lsn() {
    let versions = vec![
        VersionedFixtureRow::live(100, 1, "new"),
        VersionedFixtureRow::live(200, 1, "paid"),
        VersionedFixtureRow::deleted(300, 1),
    ];
    assert_eq!(select_visible_fixture(&versions, 250).unwrap().status.as_deref(), Some("paid"));
    assert!(select_visible_fixture(&versions, 300).is_none());
}
```

Define `VersionedFixtureRow` and `select_visible_fixture` in the test module with the same rule as production SQL: filter `source_lsn <= target`, select greatest `Version`, and return `None` when that row is a tombstone. The ignored service test remains the proof that ClickHouse SQL matches this model.

- [ ] **Step 2: Verify the unit test fails**

Run: `cargo test -p graydb-r1 clickhouse::tests::version_orders_changes_inside_and_across_commits -- --exact`

Expected: FAIL because ClickHouse versioning is absent.

- [ ] **Step 3: Add version-preserving DDL**

Create one raw version table per source table. Use source types mapped to ClickHouse types, nullable non-key columns for tombstones, `_source_lsn UInt64`, `_change_ordinal UInt32`, `_version UInt128`, and `_deleted UInt8`. Use `ReplacingMergeTree(_version)` ordered by the source primary key. Do not enable cleanup that removes history during a run.

- [ ] **Step 4: Implement initial load and CDC apply**

Initial-load rows use `source_lsn = initial_lsn`, ordinal zero, and `_deleted = 0`. For each committed pgoutput transaction, preserve stream order, assign ordinal starting at one, and POST one `JSONEachRow` batch to ClickHouse. A delete writes the key, null non-key fields, and `_deleted = 1`. Record the transaction in `r1_meta.applied_transactions` with operation hash and LSN in the same ClickHouse request sequence; acknowledge PostgreSQL only after both inserts succeed and a verification query sees the transaction marker.

On retry, query the marker first. A matching marker makes the retry idempotent; a different hash at the same LSN is a hard correctness failure. Set `non_replicated_deduplication_window = 1000000` and send stable `insert_deduplication_token` values in the form `<operation-sha256>:<table>` so a crash after a table insert but before the marker cannot create a second physical event. Verify `count() = uniqExact(_event_id)` at every checkpoint.

- [ ] **Step 5: Implement exact-at-LSN SQL**

Each query must first reduce every source table to its greatest `_version` where `_source_lsn <= {target_lsn}`, then filter `_deleted = 0`, then run the logical Q1-Q5 aggregation. Use `argMax(tuple(source_columns, _deleted), _version)` grouped by primary key. Q3 must reduce both `orders` and `tenants` before joining. Request `JSONCompactEachRowWithNamesAndTypes` and capture `read_rows`, `read_bytes`, and elapsed time from ClickHouse progress headers.

- [ ] **Step 6: Run unit and service integration tests**

Run: `cargo test -p graydb-r1 clickhouse::tests -- --nocapture`

Expected: PASS.

Run: `cargo test -p graydb-r1 --test clickhouse_cdc --no-run`

Expected: the ignored integration target compiles; Task 11 executes insert, update, delete, retry, stale-LSN, and restart cases against the service environment.

- [ ] **Step 7: Commit ClickHouse CDC**

```bash
git add crates/graydb-r1 bench/r1/clickhouse.sql bench/r1/queries/clickhouse
git commit -m "feat(r1): add exact-LSN ClickHouse CDC adapter"
```

Define `OracleFixture::five_commits()` in the test module as five single-row plans containing insert, update, and delete operations. Its `mutated` method applies exactly the selected `Mutation`. Define `scorecard_fixture(cells, p95_ratio, churn_ratio)` as two load-stage summaries with the supplied five Q1-Q5 cells and aggregate ratios.

---

### Task 9: Implement the dual correctness oracle and invalidation rules

**Files:**
- Modify: `crates/graydb-r1/src/lib.rs`
- Create: `crates/graydb-r1/src/oracle.rs`
- Create: `crates/graydb-r1/src/verdict.rs`

**Interfaces:**
- Consumes: `Operation`, `LedgerEntry`, `QueryId`, `QueryParameters`, `QueryResult`, `EngineKind`.
- Produces: `LedgerOracle::apply`, `LedgerOracle::query`, `PostgresCheckpoint::capture`, `CorrectnessVerdict`, `RunInvalidation`, `CellVerdict`, and `Scorecard::evaluate`.

- [ ] **Step 1: Write failing oracle mutation tests**

```rust
#[test]
fn oracle_rejects_each_required_corruption_class() {
    let fixture = OracleFixture::five_commits();
    for mutation in [
        Mutation::DropSequence(3),
        Mutation::DuplicateSequence(4),
        Mutation::UseVersionBeforeCheckpoint,
        Mutation::IgnoreLatestTombstone,
    ] {
        let candidate = fixture.mutated(mutation);
        let verdict = fixture.oracle.compare(&candidate);
        assert!(!verdict.passed, "mutation must fail: {mutation:?}");
        assert!(!verdict.differences.is_empty());
    }
}

#[test]
fn winner_rule_requires_four_wins_no_losses_and_both_aggregate_bounds() {
    let scorecard = scorecard_fixture([CellVerdict::GrayDbWin; 5], 0.88, 0.75);
    assert!(scorecard.graydb_beat_clickhouse());
    let with_loss = scorecard_fixture([
        CellVerdict::GrayDbWin,
        CellVerdict::GrayDbWin,
        CellVerdict::GrayDbWin,
        CellVerdict::GrayDbWin,
        CellVerdict::ClickHouseWin,
    ], 0.88, 0.75);
    assert!(!with_loss.graydb_beat_clickhouse());
}
```

Define `invalid_result(reason)` in the report test module as a complete `RunResult` with benchmark ID `R1-P1-v1`, profile `MacSmoke`, zero metric samples, `valid = false`, and one supplied invalidation.

- [ ] **Step 2: Verify the tests fail**

Run: `cargo test -p graydb-r1 oracle::tests::oracle_rejects_each_required_corruption_class -- --exact`

Expected: FAIL because the oracle module is absent.

Run: `cargo test -p graydb-r1 verdict::tests::winner_rule_requires_four_wins_no_losses_and_both_aggregate_bounds -- --exact`

Expected: FAIL because the verdict module is absent.

- [ ] **Step 3: Implement the ledger-state oracle**

Maintain keyed `BTreeMap` state for customers, orders, and events plus immutable tenants. Require contiguous sequence and matching hash before applying a transaction. Evaluate Q1-Q5 directly over that state with integer arithmetic. Return canonical rows through Task 3's encoder. Store sampled row diffs with table, primary key, expected version, actual version, and target checkpoint.

- [ ] **Step 4: Implement PostgreSQL checkpoints**

`PostgresCheckpoint::capture` must pause the writer, wait until its in-flight count is zero, begin `REPEATABLE READ READ ONLY`, capture `pg_current_wal_lsn()`, execute Q1-Q5 and deterministic primary-key samples, commit, wait for both engines at that LSN, compare canonical digests, durably emit the verdict, and resume the writer. Checkpoint time is outside query measurement windows.

- [ ] **Step 5: Implement exact invalidation and score rules**

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunInvalidation {
    DatasetHashMismatch,
    WorkloadHashMismatch,
    MissingSequence(u64),
    DuplicateSequence(u64),
    StateChangingReorder { before: u64, after: u64 },
    StaleResult { target_lsn: u64, visible_lsn: u64 },
    ResultDigestMismatch { query: QueryId, checkpoint: u64 },
    FreshnessP99Exceeded { limit_ms: u64, actual_ms: u64 },
    SourceRateMissed { target: u64, achieved: u64 },
    ResourceSafetyGate(String),
    UnexpectedProcessExit(String),
    MissingArtifact(String),
}
```

Define `ComposeContract` test-only deserialization structs for `services`, `healthcheck`, `mem_limit`, and `volumes`; `load_compose()` reads the repository-root Compose file and parses it through `serde_yaml`; its helper methods normalize `3g` and `4g` to bytes and inspect bind-source strings.

A cell win requires p95 at least 5% lower and p99 no higher. A tie requires both differences below 5%. Overall GrayDB success requires the exact three bullets in spec section 16 at 1,000 rows/s and the highest common sustainable rate.

- [ ] **Step 6: Run tests and commit**

Run: `cargo test -p graydb-r1 oracle::tests -- --nocapture`

Expected: PASS.

Run: `cargo test -p graydb-r1 verdict::tests -- --nocapture`

Expected: PASS for correct fixtures and every deliberate corruption.

```bash
git add crates/graydb-r1
git commit -m "feat(r1): enforce correctness and winner rules"
```

Define `ControllerFixture` and `FailureFixture` in `controller_state.rs` with in-memory implementations of `EngineAdapter`, `ComposeControl`, `PublishedSizeProbe`, and the writer control interface. Use Tokio's paused clock so 120-second and 1,800-second assertions run without wall-clock waiting.

---

### Task 10: Collect latency, resource, timing, and report evidence

**Files:**
- Modify: `crates/graydb-r1/src/lib.rs`
- Create: `crates/graydb-r1/src/metrics.rs`
- Create: `crates/graydb-r1/src/report.rs`

**Interfaces:**
- Consumes: `EventSink`, `ProfileSpec`, `QueryResult`, `CorrectnessVerdict`, `RunInvalidation`.
- Produces: `LatencySeries`, `StageTimer`, `ResourceSampler`, `RunResult`, `ReportWriter::write`, and `AwsCapacityRequest::from_mac_result`.

- [ ] **Step 1: Write failing percentile, timing, and report tests**

```rust
#[test]
fn latency_summary_uses_recorded_samples_without_fastest_run_selection() {
    let mut series = LatencySeries::new(3).unwrap();
    for micros in [1_000, 2_000, 3_000, 4_000, 100_000] { series.record_micros(micros).unwrap(); }
    let s = series.summary();
    assert_eq!(s.samples, 5);
    assert_eq!(s.p50_micros, 3_000);
    assert_eq!(s.max_micros, 100_000);
}

#[test]
fn invalid_run_report_never_contains_a_winner() {
    let result = invalid_result(RunInvalidation::MissingSequence(9));
    let report = ReportWriter::render_markdown(&result).unwrap();
    assert!(report.contains("INVALID"));
    assert!(!report.contains("GrayDB beat ClickHouse"));
    assert!(!report.contains("ClickHouse beat GrayDB"));
}
```

- [ ] **Step 2: Verify the tests fail**

Run: `cargo test -p graydb-r1 metrics::tests -- --nocapture`

Expected: FAIL because the metrics module is absent.

Run: `cargo test -p graydb-r1 report::tests -- --nocapture`

Expected: FAIL because the report module is absent.

- [ ] **Step 3: Implement measurement types**

Use `hdrhistogram::Histogram<u64>` with microsecond units and three significant figures. Key query series by repetition, stage, engine, and query ID. Key freshness series by repetition, stage, and engine. `StageTimer` uses `Instant` for duration and system time only for display.

Parse `docker stats --no-stream --format '{{json .}}'` once per second into:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSample {
    pub monotonic_ns: u128,
    pub service: String,
    pub cpu_percent: f64,
    pub memory_bytes: u64,
    pub block_read_bytes: u64,
    pub block_write_bytes: u64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
}
```

Also query ClickHouse asynchronous metrics and GrayDB status at stage boundaries. Raw samples go under `metrics/`; summaries go into `result.json`.

- [ ] **Step 4: Implement reports and AWS capacity input**

`result.md` must show validity first, then total time, operation-time table, dataset bytes and rows, correctness checks, query p50/p95/p99/max/sample count, freshness, source rate, recovery, CPU, memory, I/O, storage amplification, every cell verdict, and the exact overall conclusion allowed by Task 9.

`AwsCapacityRequest::from_mac_result` must calculate source, GrayDB, ClickHouse, WAL, temporary, and artifact bytes from measured amplification, apply a 1.35 safety factor, and output `aws-capacity-request.json`. It must contain `approved: false` and no AWS credentials or provisioning action.

- [ ] **Step 5: Run tests and commit**

Run: `cargo test -p graydb-r1 metrics::tests -- --nocapture`

Expected: PASS.

Run: `cargo test -p graydb-r1 report::tests -- --nocapture`

Expected: PASS.

```bash
git add crates/graydb-r1
git commit -m "feat(r1): report complete benchmark evidence"
```

---

### Task 11: Add the safe Colima and Docker Compose environment

**Files:**
- Modify: `.gitignore`
- Create: `bench/r1/compose.yml`
- Create: `bench/r1/Dockerfile`
- Create: `bench/r1/graydb-r1.toml`
- Create: `bench/r1/clickhouse/config.xml`
- Create: `bench/r1/clickhouse/users.xml`
- Create: `scripts/r1-colima.sh`
- Create: `crates/graydb-r1/tests/compose_contract.rs`

**Interfaces:**
- Consumes: R1 paths, ports, memory limits, and image versions.
- Produces: Compose services `postgres`, `graydb`, and `clickhouse`; Colima profile `r1`; host ports 55432, 57432, and 58123.

- [ ] **Step 1: Write a failing Compose contract test**

Parse `bench/r1/compose.yml` as YAML and assert:

```rust
#[test]
fn compose_has_isolated_persistent_services_and_healthchecks() {
    let compose = load_compose();
    for name in ["postgres", "graydb", "clickhouse"] {
        assert!(compose.services.contains_key(name));
        assert!(compose.services[name].healthcheck.is_some());
    }
    assert_eq!(compose.services["postgres"].memory_limit_bytes(), 3_u64 << 30);
    assert_eq!(compose.services["graydb"].memory_limit_bytes(), 4_u64 << 30);
    assert_eq!(compose.services["clickhouse"].memory_limit_bytes(), 4_u64 << 30);
    assert!(compose.all_bind_mounts_begin_with("${R1_DATA_ROOT}"));
}
```

- [ ] **Step 2: Verify the test fails**

Run: `cargo test -p graydb-r1 --test compose_contract -- --nocapture`

Expected: FAIL because Compose assets are absent.

- [ ] **Step 3: Create pinned service definitions**

Use `postgres:17` and `clickhouse/clickhouse-server:25.8`. At preflight, resolve and record their repository digests; a measured repetition may use only the digest recorded at dataset creation. Build GrayDB Studio from the current Git SHA in a Rust 1.95 multi-stage image. Build `r1ctl` in release mode on the host. Set `GRAYDB_STUDIO_BIND=0.0.0.0`, use the benchmark config, and expose only the three localhost-bound host ports.

Mount unique run directories below `${R1_DATA_ROOT}`. Do not use anonymous volumes. Give PostgreSQL 3 GiB, GrayDB 4 GiB, and ClickHouse 4 GiB; reserve the remaining Colima memory for the VM and Docker overhead. Add health checks that exercise SQL or HTTP, not process names.

- [ ] **Step 4: Create the Colima setup script**

The script must use `set -euo pipefail`, default `R1_DATA_ROOT` to `/Volumes/Crucial X9/GrayDB/.r1`, canonicalize and validate the prefix, create only named child directories, and run:

```bash
colima start --profile r1 \
  --cpu 8 \
  --memory 12 \
  --disk 600 \
  --disk-image "$R1_DATA_ROOT/colima/disk.img" \
  --mount /Volumes/Crucial\ X9/GrayDB:w
```

If the profile already runs, inspect and report it without recreating it. Never stop another Colima profile. Print `colima status --profile r1` and `docker context show` at completion.

- [ ] **Step 5: Validate configuration without starting services**

Run: `bash -n scripts/r1-colima.sh`

Expected: PASS.

Run: `R1_DATA_ROOT='/Volumes/Crucial X9/GrayDB/.r1' docker compose -f bench/r1/compose.yml config --quiet`

Expected: PASS.

Run: `cargo test -p graydb-r1 --test compose_contract`

Expected: PASS.

- [ ] **Step 6: Start services and run the deferred integration tests**

Run: `bash scripts/r1-colima.sh`

Expected: profile `r1` is running with the exact resource and external-disk settings.

Run: `R1_DATA_ROOT='/Volumes/Crucial X9/GrayDB/.r1/integration' docker compose -f bench/r1/compose.yml up -d --build --wait`

Expected: all three service health checks PASS.

Run: `cargo test -p graydb-r1 --test postgres_dataset -- --ignored --nocapture`

Expected: PASS.

Run: `cargo test -p graydb-r1 --test postgres_workload -- --ignored --nocapture`

Expected: PASS, including uncertain-commit recovery.

Run: `cargo test -p graydb-r1 --test clickhouse_cdc -- --ignored --nocapture`

Expected: PASS, including tombstone, retry deduplication, exact target LSN, and restart cases.

- [ ] **Step 7: Commit the local environment**

```bash
git add .gitignore bench/r1 scripts/r1-colima.sh crates/graydb-r1/tests/compose_contract.rs
git commit -m "build(r1): add external-disk benchmark environment"
```

---

### Task 12: Implement the resumable controller, snapshots, and failure sequence

**Files:**
- Modify: `crates/graydb-r1/src/lib.rs`
- Create: `crates/graydb-r1/src/controller.rs`
- Create: `crates/graydb-r1/src/failure.rs`
- Create: `crates/graydb-r1/src/bin/r1ctl.rs`
- Create: `crates/graydb-r1/tests/controller_state.rs`

**Interfaces:**
- Consumes: every interface from Tasks 1-11.
- Produces: `RunStage`, `RunState`, `RunController::advance`, `ComposeControl`, `BaselineSnapshot`, and CLI commands `preflight`, `seed`, `correctness`, `run`, `resume`, `report`, `estimate-aws`, `verify-artifacts`, and `self-test-invalidations`.

- [ ] **Step 1: Write failing state-machine and failure tests**

```rust
#[tokio::test]
async fn resume_starts_after_last_durable_stage_only() {
    let fixture = ControllerFixture::new();
    fixture.complete_through(RunStage::Quiet).await;
    fixture.crash_before_stage_commit(RunStage::Cdc300).await;
    let resumed = fixture.resume().await;
    assert_eq!(resumed.next_stage(), RunStage::Cdc300);
    assert_eq!(resumed.execution_count(RunStage::Quiet), 1);
}

#[tokio::test]
async fn planned_engine_kill_keeps_writes_running_and_validates_catchup() {
    let fixture = FailureFixture::new(EngineKind::Graydb);
    let result = fixture.run_engine_kill(Duration::from_secs(120)).await.unwrap();
    assert!(result.source_rows_written_while_down > 0);
    assert!(result.caught_up_within(Duration::from_secs(1_800)));
    assert!(result.correctness.passed);
}
```

- [ ] **Step 2: Verify the tests fail**

Run: `cargo test -p graydb-r1 --test controller_state -- --nocapture`

Expected: FAIL because controller types are absent.

- [ ] **Step 3: Implement the durable stage machine**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunStage {
    Preflight,
    Seed,
    BaselineSnapshot,
    Bootstrap,
    InitialCheckpoint,
    Warmup,
    Quiet,
    Cdc300,
    Cdc1000,
    RateSearch,
    FailureSequence,
    FinalCheckpoint,
    Report,
    Checksums,
    Complete,
}
```

Write `run-state.json.partial`, sync, rename, and sync its directory after every stage. A stage stores input hashes, start/end times, command outcomes, artifact paths, and validity. Resume repeats an incomplete stage and refuses to continue after an invalidation except to generate the report and checksums.

- [ ] **Step 4: Implement baseline and isolated replay sources**

After seeding and before engine slots exist, run `pg_basebackup` into `<run>/baseline/postgres` and checksum it. For isolated runs, create a fresh, previously nonexistent PostgreSQL data directory per engine from that baseline; never delete or overwrite the baseline. Replay the same intent plans, write each sequence-to-LSN map, and compare workload hashes before queries begin.

- [ ] **Step 5: Implement stage execution and stop rules**

Follow the exact order and durations in spec section 13. Query each Q1-Q5 at least 30 times per stage, extending no more than 2x. Stop the rate search on source rate below 95% for three one-minute intervals, freshness p99 above 1,000 ms, backlog above 10 GiB and growing for three intervals, correctness failure, or resource gate. Sample free space once per second and pause the writer before the 15% runtime floor.

Correctness mode runs both engines together and never emits a comparative winner. Isolated mode restores the same baseline twice, runs exactly one analytical engine at a time, applies the same workload sequence, keeps the 4 GiB engine limit and query schedule unchanged, and compares only matching logical checkpoints after both replay maps pass hash validation.

- [ ] **Step 6: Implement controlled failures**

`ComposeControl` invokes argument-vector commands without a shell. Stop the selected engine for 120 seconds while the writer stays active, start it, wait up to 30 minutes, and validate. Disconnect each CDC service from the Compose network for 60 seconds and reconnect it. Stop the writer for 30 seconds and resume from its intent/ledger state. Restart `r1ctl` by exiting with code 75 after durable state write; `resume` continues from the next incomplete stage.

- [ ] **Step 7: Implement the operator CLI**

The primary visible command is:

```bash
cargo run --release -p graydb-r1 --bin r1ctl -- \
  run --profile mac-smoke --mode correctness --engines graydb,clickhouse
```

Every subcommand accepts `--run-root`, defaults to `/Volumes/Crucial X9/GrayDB/.r1/runs`, prints the resolved run ID and log path first, and returns nonzero for invalid or incomplete runs. `--json` changes stdout formatting but never disables `run.log`.

`self-test-invalidations` runs the four Task 9 mutation fixtures, exits zero only when all four are rejected, and writes no benchmark result.

- [ ] **Step 8: Run tests and commit**

Run: `cargo test -p graydb-r1 controller::tests -- --nocapture`

Expected: PASS.

Run: `cargo test -p graydb-r1 failure::tests -- --nocapture`

Expected: PASS.

Run: `cargo test -p graydb-r1 --test controller_state -- --nocapture`

Expected: PASS with fake services and paused time.

```bash
git add crates/graydb-r1
git commit -m "feat(r1): orchestrate resumable benchmark stages"
```

---

### Task 13: Add operator commands, documentation, and the 1 GiB acceptance rehearsal

**Files:**
- Modify: `justfile`
- Modify: `README.md`
- Modify: `docs/RESEARCH-R1.md`
- Modify: `docs/MILESTONES.md`
- Modify: `docs/SETUP.md`
- Create: `docs/R1-PHASE-1-RUNBOOK.md`

**Interfaces:**
- Consumes: completed `r1ctl` and environment.
- Produces: `just r1-setup`, `just r1-unit`, `just r1-services`, `just r1-smoke`, `just r1-resume`, and the operator runbook.

- [ ] **Step 1: Add task-runner recipes**

Use POSIX `bash` for the new recipes without changing existing Windows recipes:

```make
r1-setup:
    bash scripts/r1-colima.sh

r1-unit:
    cargo test -p graydb-r1

r1-services:
    R1_DATA_ROOT="/Volumes/Crucial X9/GrayDB/.r1" docker compose -f bench/r1/compose.yml up -d --build --wait

r1-smoke:
    cargo run --release -p graydb-r1 --bin r1ctl -- run --profile mac-smoke --mode correctness --engines graydb,clickhouse

r1-resume RUN_ID:
    cargo run --release -p graydb-r1 --bin r1ctl -- resume --run-id "{{RUN_ID}}"
```

- [ ] **Step 2: Write the runbook**

Document prerequisites, external paths, exact commands, expected stage logs, safe interruption, resume, artifact inspection, invalid-run interpretation, cleanup policy, scale promotion, and the AWS approval gate. State that no command deletes a dataset or accepted run automatically. Include commands to open `run.log`, `result.md`, and `SHA256SUMS`.

- [ ] **Step 3: Update research status without rewriting history**

Keep the earlier local `bench-cdc` result labeled GrayDB-only. Add `R1-P1-v1` as the new protocol, link the design and runbook, and record that Mac numbers are diagnostic. Update the milestone only to “harness implemented” after the 1 GiB rehearsal passes.

- [ ] **Step 4: Run the complete static verification gate**

Run: `cargo fmt --all -- --check`

Expected: PASS.

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS.

Run: `cargo test --workspace`

Expected: PASS with service-dependent tests ignored.

Run: `cargo run --release -p graydb-r1 --bin r1ctl -- self-test-invalidations`

Expected: PASS after reporting detected missing change, duplicate, stale version, and tombstone error.

Run: `bash -n scripts/r1-colima.sh`

Expected: PASS.

Run: `R1_DATA_ROOT='/Volumes/Crucial X9/GrayDB/.r1' docker compose -f bench/r1/compose.yml config --quiet`

Expected: PASS.

- [ ] **Step 5: Start the approved external-disk environment**

Run: `just r1-setup`

Expected: Colima profile `r1` reports 8 CPUs, 12 GiB memory, a 600 GiB disk image under `/Volumes/Crucial X9/GrayDB/.r1/colima`, and the `r1` Docker context.

Run: `just r1-services`

Expected: PostgreSQL, GrayDB, and ClickHouse health checks PASS; the host `r1ctl` preflight reaches all three services.

- [ ] **Step 6: Run the visible 1 GiB rehearsal**

Run: `just r1-smoke`

Expected: terminal output shows every stage and duration; PostgreSQL measured published data is at least 1 GiB; live correctness has no mismatches; controlled failures recover; `result.md` is valid but explicitly non-publishable; `SHA256SUMS` verifies.

- [ ] **Step 7: Verify the artifact contract**

Run: `cargo run --release -p graydb-r1 --bin r1ctl -- verify-artifacts --latest`

Expected: PASS with zero missing files and zero checksum mismatches.

Run: `cargo run --release -p graydb-r1 --bin r1ctl -- estimate-aws --latest`

Expected: writes `aws-capacity-request.json` with `approved: false`; creates no cloud resource.

- [ ] **Step 8: Commit the verified Mac harness**

```bash
git add justfile README.md docs/RESEARCH-R1.md docs/MILESTONES.md docs/SETUP.md docs/R1-PHASE-1-RUNBOOK.md
git commit -m "docs(r1): publish Mac phase zero runbook"
```

- [ ] **Step 9: Record the execution evidence without committing generated data**

Run: `git status --short`

Expected: clean worktree; `.r1` data and `bench-results` remain ignored. Report the run ID, absolute `run.log` and `result.md` paths, total elapsed time, validity, and next eligible scale. Do not claim a ClickHouse win or loss from the Mac result.
