@echo off
rem SP3: Demo 6 (DDL in-stream, per-LSN interpretation) against PG16 (:5416).
set "PATH=%USERPROFILE%\.cargo\bin;%PATH%"
set "GRAYDB_SOURCE_PORT=5416"
cd /d "%~dp0"
cargo run -p graydb-check --bin demo-sp3
