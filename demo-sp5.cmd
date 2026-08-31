@echo off
rem SP5: tantivy search in commit-LSN batches against PG17 (:5417).
set "PATH=%USERPROFILE%\.cargo\bin;%PATH%"
cd /d "%~dp0"
cargo run -p graydb-check --bin demo-sp5
