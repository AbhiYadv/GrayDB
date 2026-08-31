# GrayDB spike — task runner. `just demo-spN` per milestone (CLAUDE.md working rule).
set windows-shell := ["powershell.exe", "-NoProfile", "-Command"]

# Bring up both source Postgres containers (16 + 17, wal_level=logical)
up:
    docker compose up -d --wait

down:
    docker compose down -v

# Local no-Docker sources (D-005): call pg_ctl.exe directly — works even when
# PowerShell script execution is disabled (no .ps1 involved).
pg-start:
    & "..\tools\pg16\pgsql\bin\pg_ctl.exe" -D "..\tools\pgdata\pg16" status | Out-Null; if ($LASTEXITCODE -ne 0) { & "..\tools\pg16\pgsql\bin\pg_ctl.exe" -D "..\tools\pgdata\pg16" -l "..\tools\pgdata\pg16\server.log" -w start }; & "..\tools\pg17\pgsql\bin\pg_ctl.exe" -D "..\tools\pgdata\pg17" status | Out-Null; if ($LASTEXITCODE -ne 0) { & "..\tools\pg17\pgsql\bin\pg_ctl.exe" -D "..\tools\pgdata\pg17" -l "..\tools\pgdata\pg17\server.log" -w start }

pg-stop:
    & "..\tools\pg16\pgsql\bin\pg_ctl.exe" -D "..\tools\pgdata\pg16" -m fast -w stop; & "..\tools\pg17\pgsql\bin\pg_ctl.exe" -D "..\tools\pgdata\pg17" -m fast -w stop

pg-status:
    & "..\tools\pg16\pgsql\bin\pg_ctl.exe" -D "..\tools\pgdata\pg16" status; & "..\tools\pg17\pgsql\bin\pg_ctl.exe" -D "..\tools\pgdata\pg17" status

test:
    cargo test --workspace

# SP1 — Demo 1: exported-snapshot initial load; graydb-check row-multiset equality at LSN0.
# Runs against pg17 (graydb.toml default).
demo-sp1:
    cargo run -p graydb-check --bin demo-sp1

# Same demo against pg16 (test both — CLAUDE.md working rule)
demo-sp1-pg16:
    $env:GRAYDB_SOURCE_PORT = "5416"; cargo run -p graydb-check --bin demo-sp1

# SP2 — Demo 2 (concurrent ingestion during load, ack == durable) +
#       Demo 8 (WAL budget rungs 1–3 under an induced stall).
demo-sp2:
    cargo run -p graydb-check --bin demo-sp2

demo-sp2-pg16:
    $env:GRAYDB_SOURCE_PORT = "5416"; cargo run -p graydb-check --bin demo-sp2

# SP3 — Demo 6: ADD COLUMN + DROP COLUMN flow through in-stream with
#       correct per-LSN interpretation, replayed from frames alone.
demo-sp3:
    cargo run -p graydb-check --bin demo-sp3

demo-sp3-pg16:
    $env:GRAYDB_SOURCE_PORT = "5416"; cargo run -p graydb-check --bin demo-sp3

# SP4 — Demo 3: update+delete via replica identity land correctly in parquet
#       segments + delete bitmaps, with target-LSN time travel.
demo-sp4:
    cargo run -p graydb-check --bin demo-sp4

demo-sp4-pg16:
    $env:GRAYDB_SOURCE_PORT = "5416"; cargo run -p graydb-check --bin demo-sp4

# SP5 — tantivy search applied in commit-LSN batches, never mid-transaction.
demo-sp5:
    cargo run -p graydb-check --bin demo-sp5

demo-sp5-pg16:
    $env:GRAYDB_SOURCE_PORT = "5416"; cargo run -p graydb-check --bin demo-sp5

# SP6 — Demo 5: caller-supplied target-LSN queries over both shapes,
#       search() table function, graydb.stat_replication.
demo-sp6:
    cargo run -p graydb-check --bin demo-sp6

demo-sp6-pg16:
    $env:GRAYDB_SOURCE_PORT = "5416"; cargo run -p graydb-check --bin demo-sp6

# SP7 — Demos 4 + 7 + source failover: decoder kill -> fresh session from durable
#       ack; crash-before-materialize replay; pg_ctl immediate restart.
demo-sp7:
    cargo run -p graydb-check --bin demo-sp7

demo-sp7-pg16:
    $env:GRAYDB_SOURCE_PORT = "5416"; cargo run -p graydb-check --bin demo-sp7

# SP8 — GrayDB Studio: the GUI. http://127.0.0.1:7432
# Demo tip: a small WAL budget makes the gauge walk the rungs on camera.
studio:
    cargo run -p graydb-studio

studio-demo:
    $env:GRAYDB_WAL_BUDGET_BYTES = "4194304"; cargo run -p graydb-studio

studio-pg16:
    $env:GRAYDB_SOURCE_PORT = "5416"; cargo run -p graydb-studio

# R1 — GrayDB column of the ClickHouse benchmark (docs/RESEARCH-R1.md).
# ALWAYS release build; scale via GRAYDB_BENCH_SEED / _TPS / _SECS.
bench-r1:
    cargo run --release -p graydb-check --bin bench-cdc
