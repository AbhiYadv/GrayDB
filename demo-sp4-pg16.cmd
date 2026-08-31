@echo off
rem SP4: Demo 3 (columnar update/delete + time travel) against PG16 (:5416).
set "PATH=%USERPROFILE%\.cargo\bin;%PATH%"
set "GRAYDB_SOURCE_PORT=5416"
cd /d "%~dp0"
cargo run -p graydb-check --bin demo-sp4
