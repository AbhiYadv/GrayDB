@echo off
rem Demo 1 against PG17 (:5417). Works even in terminals with stale PATH or
rem restricted PowerShell policy (plain cmd, no .ps1).
set "PATH=%USERPROFILE%\.cargo\bin;%PATH%"
cd /d "%~dp0"
cargo run -p graydb-check --bin demo-sp1
