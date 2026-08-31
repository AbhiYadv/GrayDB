# SETUP.md — running GrayDB on a fresh machine

Two paths. **Linux is dramatically easier** — everything below marked "Windows only" exists
because this project's dev box is Windows without admin rights, and Windows' GNU Rust
toolchain is broken for our dependency tree (see docs/DECISIONS.md D-005).

Disk: budget **~30 GB** (Rust toolchain 1.6 GB · registry/cache 0.5 GB · build target dir
**~23 GB** for debug builds of arrow/datafusion/tantivy · PostgreSQL binaries 0.8 GB each ·
llvm-mingw 0.7 GB on Windows). RAM: 8 GB works, 16 GB comfortable.

---

## Path A — Linux (recommended, and required for SP6b/pgrx)

```bash
# 1. Rust (no special toolchain gymnastics needed on Linux)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

# 2. Build prerequisites (zstd-sys and friends need a C compiler)
sudo apt-get install -y build-essential pkg-config

# 3. PostgreSQL 16 and 17 with logical replication
sudo apt-get install -y postgresql-16 postgresql-17
# in each postgresql.conf:  wal_level = logical
#                           max_replication_slots = 8
#                           max_wal_senders = 8
# then restart both clusters

# 4. Task runner (optional — the .cmd wrappers are Windows-only, use just or cargo directly)
cargo install just

# 5. Build and run
cd graydb
cargo test --workspace
cargo run -p graydb-check --bin demo-sp1      # or: just demo-sp1
cargo run -p graydb-studio                    # http://127.0.0.1:7432
```

Nothing else. No linker config, no dlltool, no llvm-mingw.

---

## Path B — Windows, no admin rights (what this machine runs)

### 1. Rust — install the **gnullvm** toolchain as default

```powershell
# rustup-init.exe from https://win.rustup.rs/x86_64
.\rustup-init.exe -y --default-host x86_64-pc-windows-gnu --profile minimal --no-modify-path
rustup toolchain install stable-x86_64-pc-windows-gnullvm --profile minimal
rustup default stable-x86_64-pc-windows-gnullvm
```

Current versions here: rustc/cargo **1.97.1**, toolchain **stable-x86_64-pc-windows-gnullvm**.

**Why not the normal `windows-gnu` or `windows-msvc` toolchain?**
- `windows-msvc` needs Visual Studio Build Tools → admin install. Not available here.
- `windows-gnu` is *broken* for this dependency tree, twice over: rustup's bundled MinGW ships
  no assembler (so `dlltool` fails on raw-dylib import libs), and when you supply an external
  dlltool, GNU `ld` mislinks the short import libraries — every tokio/windows-sys binary then
  dies at startup with `STATUS_ACCESS_VIOLATION`. Hours were lost to this; don't repeat it.

### 2. llvm-mingw (Windows only) — the C/link toolchain

Download `llvm-mingw-<date>-ucrt-x86_64.zip` from
`https://github.com/mstorsjo/llvm-mingw/releases/latest` and extract it (here:
`database\tools\llvm-mingw-20260616-ucrt-x86_64`). It provides clang, lld, llvm-ar and an
LLVM `dlltool` that needs no external assembler. Add its `bin` to PATH.

### 3. `~/.cargo/config.toml` (Windows only) — wire the toolchain together

Fix the paths to your extraction directory:

```toml
[build]
target = "x86_64-pc-windows-gnullvm"

[env]
"CC_x86_64-pc-windows-gnullvm"  = "…\\llvm-mingw-…\\bin\\x86_64-w64-mingw32-clang.exe"
"CXX_x86_64-pc-windows-gnullvm" = "…\\llvm-mingw-…\\bin\\x86_64-w64-mingw32-clang++.exe"
"AR_x86_64-pc-windows-gnullvm"  = "…\\llvm-mingw-…\\bin\\llvm-ar.exe"

[target.x86_64-pc-windows-gnullvm]
linker = "…\\llvm-mingw-…\\bin\\x86_64-w64-mingw32-clang.exe"
rustflags = ["-C", "dlltool=…\\llvm-mingw-…\\bin\\dlltool.exe",
             "-C", "target-feature=+crt-static"]
```

`+crt-static` matters: without it the binaries need `libunwind.dll` at runtime.

### 4. PostgreSQL 16 + 17, portable (no installer, no admin)

Download the binary zips (no MSI):
`https://get.enterprisedb.com/postgresql/postgresql-17.6-1-windows-x64-binaries.zip`
and `…postgresql-16.10-1-windows-x64-binaries.zip`. Extract to `tools\pg17`, `tools\pg16`, then:

```powershell
.\scripts\local-pg.ps1 -Action init -Version both    # initdb both clusters
.\scripts\local-pg.ps1 -Action start -Version both   # pg16 -> 5416, pg17 -> 5417
```

That script sets `wal_level=logical`, `max_replication_slots=8`, `max_wal_senders=8`,
scram-sha-256 auth, database `appdb`, user `postgres`, password `graydb`.

If PowerShell blocks the script (`.ps1 cannot be loaded because running scripts is disabled`),
either use the plain-exe recipes instead — `just pg-start` / `just pg-stop` / `just pg-status`,
which call `pg_ctl.exe` directly — or ask IT about `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser`.

### 5. Optional: `cargo install just`

Not required — every demo also has a `.cmd` wrapper (`demo-sp1.cmd` … `demo-sp7.cmd`,
`studio.cmd`) which sets PATH itself and works even with script execution disabled.

### 6. Build and verify

```powershell
cargo test --workspace          # ~20 tests, all green
.\demo-sp1.cmd                  # first build is slow: arrow+datafusion+tantivy, ~10-15 min cold
.\studio.cmd                    # http://127.0.0.1:7432
```

---

## Attaching to a database that is *not* the demo instance

Edit `graydb.toml`:

```toml
[source]
host = "your-host"
port = 5432
dbname = "your_db"
user = "graydb_repl"       # needs REPLICATION + CREATE on the database
password = "…"             # or set GRAYDB_SOURCE_PASSWORD
schema = "your_schema"     # the ONE schema this spike backfills
publication = "graydb_pub"
slot = "graydb_slot"

[[search.indexes]]         # repeat per table you want searchable
table = "your_schema.your_table"
columns = ["title", "body"]
```

Source-side requirements:

| Requirement | Why | If missing |
|---|---|---|
| `wal_level = logical` | logical replication | GrayDB refuses to attach (loud) |
| `REPLICATION` privilege | create/read the slot | attach fails at slot creation |
| Rights to create schema `graydb` | the `ddl_log` capture table | attach fails installing the pack |
| Superuser (for `CREATE EVENT TRIGGER`) | in-stream DDL capture | **unverified on managed providers** — see below |
| PK or replica identity per table | update/delete correctness | table degrades to append-only; Studio's Tables panel shows which |

**Managed PostgreSQL (RDS / Aurora / Cloud SQL) — untested.** PostgreSQL requires superuser
to create event triggers, and each provider's role model differs. The spec already carries the
fallback (WL2: catalog-diff-only degraded mode), but nobody has run this against a managed
instance yet. Test event-trigger creation first; treat everything else as expected to work,
since the footprint is only a publication plus SQL objects.

## Ports and env overrides

| Thing | Default | Override |
|---|---|---|
| Source port | 5417 (pg17) | `GRAYDB_SOURCE_PORT=5416` for pg16 |
| Source host / password | from graydb.toml | `GRAYDB_SOURCE_HOST`, `GRAYDB_SOURCE_PASSWORD` |
| Studio HTTP port | 7432 | `GRAYDB_STUDIO_PORT` |
| WAL budget (demo-sized) | min(50 GiB, 4 h) | `GRAYDB_WAL_BUDGET_BYTES=4194304` makes the gauge move on camera |
| Log verbosity | `info,tantivy=warn` | `RUST_LOG` |

## Data GrayDB writes (all under `graydb/data/`, all disposable)

`data/snapshot/*` staged COPY parts + manifest · `data/log/*` the durable frame log ·
`data/columnar/*` parquet segments + delete sidecars · `data/search/*` tantivy indexes ·
`data/registry/*` schema history. Delete any of it and re-attach: everything is rebuildable
from snapshot + log replay (that is invariant I3, not a convenience).

## Docker path (canonical in CLAUDE.md, unused here)

`docker-compose.yml` brings up PostgreSQL 16 and 17 with `wal_level=logical` on 5416/5417 —
identical contract to `local-pg.ps1`. Requires Docker Desktop (admin + WSL2 on Windows), which
is why this machine uses the portable-binaries path instead.
