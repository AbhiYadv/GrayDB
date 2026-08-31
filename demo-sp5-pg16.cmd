@echo off
rem SP5: tantivy search in commit-LSN batches against PG16 (:5416).
set "PATH=%USERPROFILE%\.cargo\bin;%PATH%"
set "GRAYDB_SOURCE_PORT=5416"
cd /d "%~dp0"
cargo run -p graydb-check --bin demo-sp5
