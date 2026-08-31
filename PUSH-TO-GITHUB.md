# Pushing GrayDB to your personal GitHub

This folder is the complete, clean GrayDB source — 83 files, ~840 KB. No build output,
no derived data, no secrets. Verified before packaging.

## 1. Make the repo PRIVATE

**This is not optional unless you first remove three files.** These contain your business
strategy, not code:

| File | Contains |
|---|---|
| `docs/graydb_track_a_kit_v1.md` | Named target companies, outreach playbook, funnel counters |
| `docs/graydb_wedge_spec_v0.4.md` | Pricing hypothesis, kill gates, self-ratings |
| `docs/memory.md` | Strategy, competitive positioning, venture gates |

Your own docs also state a governance rule: the **licence decision must be made before any
public code** (Apache-2 data plane / closed control plane is the stated default, never
ratified). A public push is that moment arriving whether you intend it or not.

If you want a public repo instead: delete those three files, decide the licence, add a
LICENSE file, then push.

## 2. Push (from this folder)

```bash
git init -b main
git add -A
git commit -m "GrayDB S1-lite spike: SP1-SP8 + R1 benchmark harness"
git remote add origin git@github.com:<your-user>/graydb.git   # create the repo as PRIVATE first
git push -u origin main
```

Use the SSH URL if you have keys set up, otherwise the HTTPS URL and a personal access token.

## 3. On the Mac, after cloning

```bash
git clone git@github.com:<your-user>/graydb.git && cd graydb
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh    # if Rust isn't installed
brew install postgresql@17
#   in postgresql.conf: wal_level = logical, max_replication_slots = 8, max_wal_senders = 8
brew services restart postgresql@17
createdb appdb

cargo test --workspace          # ~22 tests — re-verifies the correctness work
cargo run --release -p graydb-studio    # http://127.0.0.1:7432
```

Do **not** copy `~/.cargo/config.toml` from the Windows machine — macOS needs no linker
config, and that file's gnullvm target would break the build.

`graydb.toml` defaults to port 5417 (the Windows demo layout). Homebrew PostgreSQL listens on
**5432**, so either set `GRAYDB_SOURCE_PORT=5432` or edit the `[source]` block.

## 4. What the Mac unblocks

Two things that are impossible on the Windows work laptop:

- **SP6b** — the pgrx extension that makes GrayDB queryable from `psql`. pgrx supports macOS.
- **The R1 ClickHouse column** — ClickHouse ships macOS builds, so the head-to-head in
  `docs/RESEARCH-R1.md` becomes runnable:
  ```bash
  curl https://clickhouse.com/ | sh          # single binary
  ./clickhouse server                        # then ./clickhouse client
  ```

Scale honestly: a laptop gets a credible 10–50M-row comparison. The headline 1B-row run in
the protocol still wants a rented Linux box.

## 5. Start here

`README.md` → `docs/MILESTONES.md` (what is proven and what is not) →
`docs/RESEARCH-R1.md` (the next research target).
