# R1 Phase 1 Benchmark Design

- Status: review draft, implementation pending
- Benchmark identity: `R1-P1-v1`
- Locked seed: `20260901`
- Date: 2026-09-01

## 1. Decision

R1 Phase 1 measures GrayDB against ClickHouse under continuous PostgreSQL change data capture (CDC). It tests two independent claims:

1. **Analytics:** exact analytical query latency remains lower or more stable as CDC churn rises.
2. **CDC durability:** GrayDB preserves a continuous, ordered, duplicate-free PostgreSQL log sequence number (LSN) history and recovers predictably after failure.

The benchmark never combines these claims into one score. A fast but incorrect result is a failed run. A durable result does not prove faster analytics.

The official Phase 1 result runs on Linux in AWS against at least 1 TiB of published PostgreSQL table data. The Mac run is Phase 0: it validates the harness, exposes GrayDB defects, estimates time and storage, and produces no publishable winner.

## 2. Goals and non-goals

### Goals

- Compare GrayDB and ClickHouse from the same PostgreSQL source state and the same ordered CDC transaction stream.
- Measure quiet and sustained-churn analytical latency at exact source LSNs.
- Prove inserts, updates, and deletes produce identical logical results in PostgreSQL, GrayDB, and ClickHouse.
- Measure freshness, throughput, resource use, storage amplification, failure recovery, and total elapsed time.
- Preserve terminal-readable logs and machine-readable evidence for every operation.
- Make each accepted result reproducible from a frozen dataset manifest, workload manifest, software revision, and configuration bundle.

### Non-goals

- Claim that GrayDB outperforms ClickHouse before an accepted AWS run.
- Tune one engine after observing its result without rerunning both engines under a new benchmark revision.
- Use generated row count as a substitute for measured stored data size.
- Use Kubernetes, Minikube, an LLM, or an interactive agent in the timed data path.
- Treat the Mac run as a capacity or production result.
- Include search, vector retrieval, joins outside the fixed suite, or multi-node scaling in Phase 1.

## 3. Immutable benchmark identity

The first accepted AWS result freezes `R1-P1-v1`. Its schema, generator, seed, workload mix, queries, correctness rules, and metric definitions must never change in place. A material change creates `R1-P1-v2` or a later phase and retains all earlier artifacts.

Every run receives a unique ID:

```text
r1-p1-v1-<environment>-<UTC timestamp>-<git short SHA>
```

Accepted results must identify the benchmark revision in tables, charts, filenames, and written conclusions.

## 4. Canonical dataset

### 4.1 Size contract

The official dataset contains at least **1 TiB**, equal to 1,099,511,627,776 bytes, of PostgreSQL published-table storage. This is approximately 1.10 decimal TB.

After loading and `ANALYZE`, and before starting CDC, the controller records:

```sql
SELECT sum(pg_table_size(c.oid))::bigint AS published_table_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_publication_tables p
  ON p.schemaname = n.nspname
 AND p.tablename = c.relname
WHERE p.pubname = 'graydb_r1_pub';
```

The value includes each table's main fork, TOAST data, free-space map, and visibility map. It excludes indexes, WAL, temporary files, engine copies, and benchmark artifacts. Index bytes and total PostgreSQL cluster bytes are recorded separately. Row count is reported but does not define scale.

### 4.2 Logical model

The published schema is `r1` and contains four related tables:

| Table | Purpose | Fields |
| --- | --- | --- |
| `tenants` | multi-tenant selectivity | `tenant_id bigint`, `region text`, `plan text`, `created_at timestamptz`, `settings jsonb` |
| `customers` | high-cardinality grouping | `customer_id bigint`, `tenant_id bigint`, `segment text`, `email_domain text`, `profile jsonb`, `created_at timestamptz` |
| `orders` | mutable business state | `order_id`, `tenant_id`, `customer_id`, `status`, `channel`, `amount_cents`, `created_at`, `updated_at`, `attributes` |
| `order_events` | append-heavy history | `event_id`, `order_id`, `tenant_id`, `event_type`, `event_at`, `metadata` |

IDs and `amount_cents` are `bigint`; names and categories are `text`; timestamps are `timestamptz`; and `attributes` and `metadata` are `jsonb`. Integer cents keep sums exact across engines and prevent floating-point behavior from masquerading as an LSN error. Every column is `NOT NULL`. All tables have stable primary keys and `REPLICA IDENTITY DEFAULT`. The generator and writer enforce relationships without source foreign keys, preventing cascade operations from changing the frozen workload mix. The publication includes all four tables and all operations.

Data generation follows deterministic, skewed distributions:

- Tenant activity follows a bounded Zipf distribution so a small tenant set carries substantial traffic.
- Customer and order ownership remain referentially valid.
- Timestamps cover 365 days, with recent data weighted more heavily.
- Status, region, channel, and event-type values use fixed dictionaries and non-uniform frequencies.
- Amounts use a deterministic long-tailed distribution in integer cents.
- JSON fields contain bounded, structured application metadata. They are not random padding.
- Text and JSON sizes follow a fixed distribution whose limits and histogram are stored in the manifest.

The generator streams deterministic COPY batches into PostgreSQL until the measured published-table size reaches the requested scale. It does not reuse engine-native sample datasets or engine-specific encodings.

### 4.3 Scale profiles

The logical dataset is identical at every scale; only its deterministic row-ID range is truncated.

| Profile | Minimum published-table bytes | Purpose |
| --- | ---: | --- |
| `mac-smoke` | 1 GiB | startup, schema, logging, and end-to-end correctness |
| `mac-correctness` | 10 GiB | full query suite and failure injection |
| `mac-validation` | 50 GiB | benchmark repeatability and resource instrumentation |
| `mac-stress` | 100 GiB | required local stress gate |
| `mac-ceiling` | 200 GiB | optional only after storage and time gates pass |
| `aws-phase1` | 1 TiB | official Phase 1 result |

## 5. Dataset manifest and reproducibility

The seed tool writes an immutable JSON manifest containing:

- benchmark ID, generator version, Git SHA, and seed;
- schema SQL and SHA-256 hash;
- table row counts, `pg_table_size`, `pg_indexes_size`, and total relation size;
- dictionary versions and distribution parameters;
- per-batch row range, row count, byte count, and checksum;
- PostgreSQL version, image digest, settings, extensions, and host details;
- publication name, replication slot names, initial snapshot ID, and initial LSN;
- load start, load finish, `ANALYZE` duration, and total elapsed time;
- storage paths and available capacity before and after loading.

The controller refuses to compare runs whose dataset-manifest hashes differ.

## 6. Canonical application writer

A compiled deterministic writer represents the application. OpenCode and other LLM clients may help implement, review, or operate the harness, but they never generate live benchmark transactions.

The writer uses PostgreSQL transactions and a frozen operation mix:

- 90% inserts;
- 8% updates;
- 2% deletes.

The mix is counted by affected logical rows, not SQL statements. Inserts target `order_events`, `orders`, and `customers` in a 60/35/5 ratio. Updates target `orders` and `customers` in a 90/10 ratio. Deletes target `orders` and `order_events` in an 80/20 ratio. `tenants` remain stable dimensions during Phase 1.

Transaction sizes follow a fixed distribution: 95% contain one row, 4% contain 10 rows, 0.9% contain 100 rows, and 0.1% contain 1,000 rows. A token-bucket limiter controls affected rows per second and records intended and achieved rates. Relationships, target keys, changed columns, values, and a deterministic application clock derive from the seed and monotonic operation sequence.

Each transaction writes its sequence and operation hash into an unscored control table in the same PostgreSQL transaction as the application changes. A dedicated control publication and logical-decoding slot map that marker to the transaction's commit-end LSN. GrayDB and ClickHouse do not ingest the control table. The controller then appends a ledger record containing the sequence, transaction identity, operation hash, affected keys, commit result, and commit-end LSN.

The checksummed ledger becomes the sole replay input for isolated engine runs. Failed and retried transactions remain visible in the event log but never appear as committed operations.

The writer supports target rates of 300 and 1,000 changed rows per second, then an increasing-rate search. Each rate runs for a fixed warmup and measurement window. The search stops when freshness, correctness, or resource safety fails.

## 7. Execution modes and fairness

### 7.1 Correctness mode

PostgreSQL, GrayDB, and ClickHouse run together. They consume the same initial source snapshot and live transaction stream. This mode proves semantic equivalence, measures simultaneous freshness, and executes failure scenarios. Its performance numbers diagnose behavior but do not decide the winner because the engines share host resources on Mac.

### 7.2 Isolated performance mode

GrayDB and ClickHouse run one at a time from the same snapshot and committed transaction ledger. Each engine receives the same CPU, memory, storage class, workload rate, warmup, query order, and measurement duration. PostgreSQL and the driver keep their fixed resource assignments.

The two isolated replays compare the same logical commit sequence. PostgreSQL may assign different numeric LSNs after a restore, so each replay stores a sequence-to-LSN map. Queries compare the same logical checkpoint and remain exact at that replay's source LSN. Correctness mode, where both engines consume one live source, compares the same numeric source LSN directly.

The `mac-validation`, `mac-stress`, `mac-ceiling`, and `aws-phase1` profiles run three measured repetitions after an unscored warmup. Smoke and correctness profiles run once because they produce no comparative conclusion. The report shows every repetition, the median repetition, and variance. It never selects the fastest repetition.

Configuration is allowed only before the run and must be disclosed. Engine-specific options may achieve the required semantics, but the controller freezes them in the run bundle. Any configuration change requires rerunning both engines.

## 8. Local Mac topology and storage safety

Phase 0 uses a dedicated Colima profile and Docker Compose. Kubernetes and Minikube are excluded.

Services are:

- PostgreSQL source;
- GrayDB;
- ClickHouse;
- deterministic application writer and ledger recorder;
- query driver;
- correctness validator and metrics collector.

The external Crucial X9 stores the dedicated Colima disk image and all benchmark artifacts. The profile has a maximum virtual disk size of 600 GiB and receives 8 virtual CPUs and 12 GiB of memory on the current 10-core, 16 GiB Apple M4 host. Large isolated runs retain one engine copy at a time.

The preflight gate aborts before allocation when any condition fails:

- the expected peak cannot leave at least 20% of the external disk free;
- the external disk cannot sustain a write-and-fsync probe;
- the data path resolves outside the approved external-volume root;
- Docker or Colima reports a smaller disk limit than the requested profile needs;
- Colima cannot reserve 8 virtual CPUs and 12 GiB of memory;
- another benchmark process or prior unarchived run owns the selected run directory.

During a run, the controller stops new writes and marks the run invalid before free space falls below 15%. It never deletes source data or accepted artifacts automatically.

## 9. AWS topology

The accepted `aws-phase1` run uses dedicated Linux EC2 hosts:

1. PostgreSQL source;
2. GrayDB;
3. ClickHouse;
4. driver, controller, validator, and metrics collector.

Each database runs as one native process or one container with host networking. Local NVMe or equivalent storage uses the same documented class and filesystem policy for both analytical engines. The controller records instance type, CPU model, memory, kernel, filesystem, mount options, storage device, image digest, region, and availability zone.

No database shares CPU or memory with its competitor in isolated mode. Network placement and security rules are symmetrical. The final artifact bundle is uploaded to versioned object storage only after local checksums complete.

## 10. ClickHouse CDC representation

ClickHouse stores one immutable version per PostgreSQL row change. Every version includes:

- source primary key;
- source columns;
- `_source_lsn` as the PostgreSQL commit-end LSN;
- `_change_ordinal` for deterministic ordering inside a transaction;
- `_version`, a monotonic value derived from `(source_lsn, change_ordinal)`;
- `_deleted`, a tombstone flag.

The table uses `ReplacingMergeTree` or an equivalent version-preserving MergeTree design. Deletes remain queryable as tombstones until retention permits cleanup.

Scored queries must be exact at target LSN `X`. The ClickHouse query first restricts versions to `_source_lsn <= X`, chooses the greatest version for each primary key, excludes tombstones, and then executes the analytical aggregation. `FINAL`, `argMax`, or an equivalent exact plan is permitted only when its semantics match this rule. The artifact bundle stores the DDL, SQL, settings, and `EXPLAIN` output.

An optional latest-state ClickHouse track may report native operational performance. It is labeled diagnostic and never substitutes for the exact-LSN score.

## 11. Query suite

The controller chooses logical checkpoint `C` before each scored query. In correctness mode, it supplies the same numeric target LSN `X` to both engines. In isolated mode, it maps `C` to each replay's source LSN and compares the same logical state. It records the checkpoint, query start, target LSN, engine-visible LSN, completion, row count, result digest, bytes read, and failure.

The fixed suite is:

| ID | Shape | Selectivity |
| --- | --- | --- |
| Q1 | seven-day revenue in cents and order count grouped by `customer_id` | high-cardinality, time-bounded |
| Q2 | order count grouped by `status` for one tenant | selective tenant filter |
| Q3 | revenue in cents and count grouped by `region`, `channel`, and `status` | multi-dimension aggregation |
| Q4 | event count grouped by `event_type` for a fixed tenant set and 24-hour window | append-heavy time range |
| Q5 | current order count and amount in cents grouped by status after updates and deletes | exact current-state stress |

The versioned query asset contains the exact SQL. Its logical forms are:

```sql
-- Q1
SELECT customer_id, sum(amount_cents), count(*)
FROM r1.orders
WHERE created_at >= :window_end - interval '7 days'
GROUP BY customer_id;

-- Q2
SELECT status, count(*)
FROM r1.orders
WHERE tenant_id = :tenant_id
GROUP BY status;

-- Q3
SELECT t.region, o.channel, o.status, sum(o.amount_cents), count(*)
FROM r1.orders o
JOIN r1.tenants t ON t.tenant_id = o.tenant_id
GROUP BY t.region, o.channel, o.status;

-- Q4
SELECT event_type, count(*)
FROM r1.order_events
WHERE tenant_id IN (:tenant_set)
  AND event_at >= :window_end - interval '24 hours'
GROUP BY event_type;

-- Q5
SELECT status, count(*), sum(amount_cents)
FROM r1.orders
GROUP BY status;
```

`:window_end`, `:tenant_id`, and `:tenant_set` derive from the seed, logical checkpoint, and deterministic application clock. They never use wall-clock `now()`. The query order follows a deterministic shuffled schedule. Every engine receives the same schedule. Results use canonical column ordering, integer representation, null handling, and row ordering before hashing.

## 12. Exactness and LSN oracle

Correctness uses two independent proofs.

### 12.1 Continuous ledger oracle

The validator reconstructs logical row state from the initial manifest and committed transaction ledger. It can evaluate sampled keys and aggregate expectations at any recorded commit-end LSN. In correctness mode, it compares both engines at the same numeric LSN. In isolated mode, it compares each engine with the oracle at the same logical checkpoint and that replay's mapped LSN.

### 12.2 PostgreSQL checkpoint oracle

At scheduled checkpoints outside timed windows, the controller pauses the canonical writer, drains in-flight transactions, captures the source LSN, and queries PostgreSQL in a repeatable-read transaction. It waits for both engines to reach that LSN, runs the same canonical queries, and compares row-level samples, counts, sums, and full result digests. The writer resumes only after the checkpoint record is durable.

The ledger and PostgreSQL checkpoint results must agree. Any missing LSN range, duplicate change, reordering that changes state, unexpected tombstone, row mismatch, or digest mismatch invalidates the run.

Freshness is measured separately as the elapsed time from PostgreSQL commit acknowledgement to exact visibility at the commit-end LSN. Time synchronization and clock source are recorded; same-host intervals use a monotonic clock.

## 13. Workload stages

Each measured repetition follows this order:

1. preflight and environment capture;
2. initial snapshot restore and engine bootstrap;
3. correctness checkpoint at the initial LSN;
4. unscored cache warmup;
5. quiet query window;
6. 300 rows-per-second CDC window;
7. 1,000 rows-per-second CDC window;
8. increasing-rate search through 2,000, 4,000, 8,000, 16,000, 32,000, and 64,000 changed rows per second until a stop condition;
9. failure and recovery sequence in correctness mode;
10. final checkpoint, resource capture, checksums, and archive.

The frozen timing profiles are:

| Profile | Repetitions | Warmup | Quiet | 300 rows/s | 1,000 rows/s | Each rate-search step | Maximum search rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `mac-smoke` | 1 | 1 min | 2 min | 2 min | 2 min | 2 min | 2,000 rows/s |
| `mac-correctness` | 1 | 2 min | 5 min | 5 min | 5 min | 3 min | 4,000 rows/s |
| `mac-validation` | 3 | 5 min | 10 min | 10 min | 10 min | 5 min | 8,000 rows/s |
| `mac-stress` | 3 | 10 min | 15 min | 20 min | 20 min | 10 min | 16,000 rows/s |
| `mac-ceiling` | 3 | 10 min | 15 min | 20 min | 20 min | 10 min | 16,000 rows/s |
| `aws-phase1` | 3 | 15 min | 30 min | 30 min | 30 min | 15 min | 64,000 rows/s |

A stage requires at least 30 successful samples from each query. It extends until it reaches that count or twice its scheduled duration; failure to reach the count invalidates the repetition. Dry-run time estimates do not count as benchmark results.

The rate search stops when the writer achieves less than 95% of target for three consecutive one-minute intervals, either engine's freshness p99 exceeds 1,000 ms, the applied backlog grows for three intervals and exceeds 10 GiB, correctness fails, or a resource safety gate fires. The last stage that both engines complete is the highest common sustainable rate.

## 14. Failure and recovery sequence

Correctness mode performs controlled failures while the writer sustains 1,000 changed rows per second. Before each failure, the system reaches steady state for two minutes. An engine stays down for 120 seconds and must catch up within 30 minutes after restart. A connection interruption lasts 60 seconds; a writer interruption lasts 30 seconds.

The sequence is:

1. terminate GrayDB while PostgreSQL continues writing;
2. restart GrayDB, measure catch-up, and validate the pre-failure through post-recovery LSN range;
3. terminate ClickHouse while PostgreSQL continues writing;
4. restart ClickHouse, measure catch-up, and run the same validation;
5. interrupt each CDC connection without stopping its engine;
6. restart the writer from its durable sequence and prove committed operations are neither lost nor repeated;
7. perform a clean controller restart and resume from the run manifest.

For each event, the controller records the command, signal, timestamp, source LSN, last received and applied LSN, restart duration, catch-up duration, replay counts, and validation result.

## 15. Metrics

### Analytics scoreboard

- Q1-Q5 latency p50, p95, p99, maximum, and sample count;
- latency ratio between quiet and each CDC rate;
- completed queries per second;
- bytes and rows read per query;
- CPU time, peak resident memory, and disk I/O per engine;
- background merge or compaction CPU and I/O;
- cold-start, bootstrap, and warmup durations.

### CDC and durability scoreboard

- source-commit-to-visible latency p50, p95, p99, and maximum;
- sustained changed rows and transactions per second;
- received and applied LSN progression;
- backlog bytes and catch-up rate;
- restart and recovery time;
- missing, duplicate, out-of-order, and replayed operations;
- update and delete amplification;
- source, engine, and artifact storage bytes and amplification ratios.

### Run timing

The report includes wall-clock duration for every stage and operation, plus total execution time from preflight start through artifact checksum completion. Generation, loading, snapshotting, bootstrap, warmup, measurement, recovery, validation, and cleanup are separate values.

## 16. Validity and winner rules

A run is accepted only when:

- all dataset and workload hashes match;
- all required repetitions complete;
- all exactness probes pass;
- no LSN gap, duplicate, or state-changing reorder occurs;
- the source meets its target workload rate;
- no resource safety gate fires;
- no process crashes outside the planned failure sequence;
- logs, metrics, configuration, and checksums are complete.

One correctness failure voids all performance numbers from that run. Invalid results remain archived and labeled with the exact reason.

For a query-stage cell, an engine wins when its median-repetition p95 is at least 5% lower and its p99 is no higher. A cell is a tie when both p95 and p99 differ by less than 5%. Every other cell reports the engine with the lower p95 or p99 and identifies the conflicting tail result instead of forcing a win.

The statement “GrayDB beat ClickHouse under CDC” is permitted only when, at 1,000 rows per second and the highest common sustainable rate:

- GrayDB wins at least four of Q1-Q5 and loses none at each rate;
- GrayDB's geometric-mean p95 across Q1-Q5 is at least 10% lower at each rate; and
- GrayDB's geometric-mean p99 churn ratio, calculated as CDC p99 divided by quiet p99, is at least 20% lower at each rate.

The primary R1 conclusion uses the complete query suite and reports every win, loss, tie, and conflicting tail result. The CDC durability conclusion is reported separately as pass or fail with recovery measurements.

No public statement may say “GrayDB beat ClickHouse” unless the accepted AWS result passes every validity rule and the complete scorecard supports that wording.

## 17. Logs and evidence

Every command streams human-readable output to the terminal and to an append-only log. The same run emits JSON Lines events for automated analysis. Required artifacts include:

```text
bench-results/<run-id>/
  run.log
  events.jsonl
  dataset-manifest.json
  workload-manifest.json
  environment.json
  configs/
  ddl/
  queries/
  explain/
  metrics/
  correctness/
  failure-events/
  result.json
  result.md
  SHA256SUMS
```

`run.log` prints stage boundaries, progress, row and byte counters, operation durations, target and applied LSNs, query samples, failures, and the final validity verdict. Secrets, credentials, and connection passwords are redacted before writing or displaying logs.

## 18. Promotion gates

The Mac profiles run in order. A profile advances only when the previous profile:

- passes every correctness and recovery check;
- completes without storage or memory pressure;
- has complete artifacts and reproducible hashes;
- produces runtime and storage estimates within the next profile's safety budget.

The 200 GiB profile is optional. It runs only when the 100 GiB result leaves the external disk above its safety floor and predicts completion within the approved local window.

AWS provisioning begins only after the 100 GiB Mac gate passes, the 1 TiB capacity estimate includes PostgreSQL, both engine copies, WAL, temporary space, and artifacts, and the expected cloud cost is presented for approval.

## 19. Implementation boundaries

Implementation will add:

- a versioned R1 schema and deterministic seed generator;
- a compiled application writer and durable transaction ledger;
- ClickHouse schema, CDC consumer, and exact-LSN query adapter;
- a controller for Colima/Docker Compose and AWS execution;
- query, validation, metrics, recovery, and artifact modules;
- preflight storage and resource guards;
- unit and integration tests for deterministic generation, replay, version selection, tombstones, LSN barriers, result canonicalization, resume, and invalidation;
- updated R1 research, milestone, setup, and runbook documents.

The existing `bench-cdc` local GrayDB measurement and its artifacts remain historical evidence. They are not renamed as Phase 1 results and do not enter the head-to-head scorecard.

## 20. Acceptance criteria

The implementation is ready for the Mac Phase 0 run when it can:

1. create and verify the 1 GiB dataset twice with identical logical hashes;
2. run PostgreSQL, GrayDB, and ClickHouse through one visible command;
3. show live progress and preserve complete logs;
4. apply the frozen transaction ledger to both engines;
5. execute Q1-Q5 at one numeric target LSN in correctness mode and matching logical checkpoints in isolated mode;
6. detect an intentionally introduced missing change, duplicate, stale version, and tombstone error;
7. recover both engines from controlled interruption without an LSN gap;
8. report per-operation and total elapsed time;
9. mark any incorrect run invalid and exclude it from winner calculations;
10. pass repository tests and a clean 1 GiB end-to-end rehearsal.
