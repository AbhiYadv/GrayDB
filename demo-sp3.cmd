@echo off
rem SP3: Demo 6 (DDL in-stream, per-LSN interpretation) against PG17 (:5417).
set "PATH=%USERPROFILE%\.cargo\bin;%PATH%"
cd /d "%~dp0"
cargo run -p graydb-check --bin demo-sp3
