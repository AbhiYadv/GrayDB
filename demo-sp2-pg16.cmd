@echo off
rem SP2: Demo 2 + Demo 8 against PG16 (:5416). PATH-safe, policy-safe.
set "PATH=%USERPROFILE%\.cargo\bin;%PATH%"
set "GRAYDB_SOURCE_PORT=5416"
cd /d "%~dp0"
cargo run -p graydb-check --bin demo-sp2
