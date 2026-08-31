@echo off
rem SP2: Demo 2 + Demo 8 against PG17 (:5417). PATH-safe, policy-safe.
set "PATH=%USERPROFILE%\.cargo\bin;%PATH%"
cd /d "%~dp0"
cargo run -p graydb-check --bin demo-sp2
