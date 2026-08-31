# DEMO.md — the 8-minute GrayDB moment (scripted runbook)

Audience: an engineer running PostgreSQL → Debezium/Kafka → Elasticsearch/ClickHouse.
Goal of the call: they ask "could this delete our pipeline?" — not "nice demo".
Everything below is real: real logical replication, real parquet + tantivy on disk, real kills.

## Before the call (5 minutes, off camera)

```
cd C:\Users\abhishek.yadav\Downloads\database\graydb
just pg-start                      # or .\scripts\local-pg.ps1 -Action start
cargo build -p graydb-studio       # warm the binary; never build on camera
.\studio.cmd                       # http://127.0.0.1:7432 (WAL budget 4 MiB so the gauge moves)
```

Open the browser at 127.0.0.1:7432 **detached** (do not attach yet — attaching is beat 1).
Have a psql window ready:

```
$env:PGPASSWORD='graydb'
..\tools\pg17\pgsql\bin\psql.exe -h 127.0.0.1 -p 5417 -U postgres -d appdb
```

Reset between runs: click Attach again (it drops the slot/publication and re-seeds).

---

## Beat 1 · 0:00–1:00 — "It attaches. It installs nothing."

Say: *"This is a stock PostgreSQL 17. GrayDB is not installed in it. Watch what attaching costs you."*

Click **Attach + backfill**. Point at the event log as lines land:
- publication + `graydb.ddl_log` event triggers — **SQL objects only**, no extension, no plugin, works on RDS/Aurora/Cloud SQL
- slot created with an **exported snapshot** → LSN0 appears
- backfill: ~32k rows, 8 parallel ctid-range COPY streams, **at exactly LSN0**

Say: *"Two SQL objects. That's the entire footprint on your database."*

## Beat 2 · 1:00–2:00 — "The backfill is exact, and the stream never stopped."

In psql:

```sql
INSERT INTO app.orders (customer_id, status, amount)
SELECT 1 + (g % 5000), 'live-traffic', 9.99 FROM generate_series(1, 2000) g;
```

Point at **Tables**: rows visible climbs; `received_lsn` and `applied_lsn` track each other; apply lag in bytes.

Say: *"Backfill ran at LSN0 while the stream was already consuming. No stop-the-world, no lock, no window where writes are lost."*

## Beat 3 · 2:00–3:15 — "Every query proves which database it answered."

Run (class **eventual**):

```sql
SELECT status, count(*) AS n FROM app.orders GROUP BY status ORDER BY n DESC
```

Point at the **LSN proof footer**. Then switch class to **strong** and run again.

Say: *"Strong means source barrier: we ask your primary for its current WAL position, wait until both shapes have passed it, then answer. The footer is the receipt — no other analytics store hands you that."*

## Beat 4 · 3:15–4:15 — "Ask it what was true five minutes ago."

Copy an older LSN from the event log (or from the earlier proof footer). Select class **target_lsn = …**, paste it, run:

```sql
SELECT count(*) AS n FROM app.orders
```

Say: *"Same store, historical answer, exact. This is a delete bitmap plus per-row insert LSNs — not a snapshot copy."*

## Beat 5 · 4:15–5:15 — "Search is the same log, in commit order."

```sql
SELECT c.id, c.name, s.score
FROM search('app.customers', 'zephyr') s
JOIN app.customers c ON CAST(c.id AS VARCHAR) = s.key
ORDER BY s.score DESC LIMIT 10
```

Then in psql:

```sql
UPDATE app.customers SET name = 'xylophone marmot' WHERE id = 42;
```

Re-run the search for `xylophone`, then for the old token — gone.

Say: *"One log feeds columnar and search. They can't disagree; they're derived from the same frames in the same order. This is where the Debezium+Kafka+ES triangle collapses into one thing."*

## Beat 6 · 5:15–6:30 — "Now break it while it's running." (the moment)

Click **Kill decoder**. In psql, keep writing:

```sql
INSERT INTO app.orders (customer_id, status, amount)
SELECT 1 + (g % 5000), 'while-dead', 1.11 FROM generate_series(1, 300) g;
```

Point at the header pill: *decoder down*, apply lag frozen, WAL gauge starting to climb.

Click **Restart from ack**. Read the event line aloud:

> *fresh replication session from last durable ack … (Relation metadata re-emitted)*

Say: *"We never splice a dying session. We resume from the last transaction-complete, checksummed, fsync'd frame — Postgres re-sends the schema metadata, so the replay is self-describing. Zero gap, zero duplicate."*

Prove it — run with class **strong**:

```sql
SELECT count(*) AS n FROM app.orders
```

Compare to psql `SELECT count(*) FROM app.orders`. **Identical.**

## Beat 7 · 6:30–7:15 — "The source is sacred, even when we're broken."

Click **Stall log write path**. Keep the psql insert loop going. Watch the **WAL budget gauge** walk: green → **rung 1 warn (50%)** → **rung 2 shed (70%)**, `spilled` frames counting up.

Say: *"Our write path is degraded, so frames divert to staging and the slot ack freezes — deliberately. The budget is a hard number: min(50 GB, 4 h) by default. Your database is never asked to keep WAL forever because we're having a bad day."*

Click **Resume log write path** → staging drains in order, ack catches up, and with the insert loop still running the gauge falls back to green.

> Keep at least a trickle of writes going during this beat. Retained WAL is released by PostgreSQL's own slot bookkeeping — `restart_lsn` advances only when a commit flows after a standby snapshot — so on a completely idle source the gauge stays high for a while even though GrayDB has fully caught up. That is honest behavior, not a bug: GrayDB will not write into a customer's database to make its own dashboard look better. If asked, say exactly that.

## Beat 8 · 7:15–8:00 — "And if your primary dies?"

Click **Crash-restart source** (confirm the dialog). This is `pg_ctl -m immediate` — a real crash, not a clean shutdown. When it's back, click **Restart from ack**, then run the strong count again against psql.

Close with: *"Same log, three replication sessions, one crash of your primary, and the numbers still match exactly. The pipeline you'd replace has none of these guarantees — and you maintain it."*

Then stop talking and ask: **"What does your current PG→ES/CH pipeline cost you per month in engineer-hours?"**

---

## Failure playbook (things that can bite on camera)

| Symptom | Cause | Fix, live |
|---|---|---|
| Attach fails with "slot active" | a previous decoder still attached | click Attach again; it drops and recreates |
| `cargo` not found in a fresh terminal | PATH added this session | use `.\studio.cmd` (sets PATH itself) |
| Strong read errors "timed out" | pump not running (you killed it) | click Restart from ack first |
| Gauge stuck at 0% | source idle, `restart_lsn` lazy | insert a few rows; it moves within a second |
| bounded(5s) fast-errors | apply lag genuinely exceeds the bound | that IS the contract; say so and switch to eventual |
| Studio shows detached after a source crash | expected | Restart from ack |

## What NOT to claim on the call

- Not a system of record — the source PostgreSQL is the only writer (I1).
- Vectors/HNSW are **not** in this build (BM25 full-text only).
- The pgrx extension surface (querying GrayDB through stock PG) is **not** in this build — Studio is the demo-grade reader. Say "the extension is next, and here's the design" if asked.
- Numbers on screen are a laptop with two local PostgreSQL instances and dev-profile builds. Don't quote them as benchmarks.
- All 20 DDL patterns are not proven — two are (ADD COLUMN, DROP COLUMN, in-stream, per-LSN).
