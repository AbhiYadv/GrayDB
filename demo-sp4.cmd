@echo off
rem SP4: Demo 3 (columnar update/delete + time travel) against PG17 (:5417).
set "PATH=%USERPROFILE%\.cargo\bin;%PATH%"
cd /d "%~dp0"
cargo run -p graydb-check --bin demo-sp4
