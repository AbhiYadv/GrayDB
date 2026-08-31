@echo off
rem SP7: chaos demos (decoder kill, crash-before-materialize, source failover) against PG17 (:5417).
rem NOTE: restarts the local PG17 instance with crash semantics as part of the demo.
set "PATH=%USERPROFILE%\.cargo\bin;%PATH%"
cd /d "%~dp0"
cargo run -p graydb-check --bin demo-sp7
