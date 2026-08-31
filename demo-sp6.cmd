@echo off
rem SP6: Demo 5 (target-LSN reader over both shapes) against PG17 (:5417).
set "PATH=%USERPROFILE%\.cargo\bin;%PATH%"
cd /d "%~dp0"
cargo run -p graydb-check --bin demo-sp6
