@echo off
rem GrayDB Studio on http://127.0.0.1:7432 (PG17 source, demo-sized WAL budget so the
rem gauge walks the ladder rungs on camera). Ctrl+C to stop.
set "PATH=%USERPROFILE%\.cargo\bin;%PATH%"
set "GRAYDB_WAL_BUDGET_BYTES=4194304"
cd /d "%~dp0"
cargo run -p graydb-studio
