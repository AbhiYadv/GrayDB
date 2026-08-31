@echo off
rem SP7: chaos demos (decoder kill, crash-before-materialize, source failover) against PG16 (:5416).
rem NOTE: restarts the local PG16 instance with crash semantics as part of the demo.
set "PATH=%USERPROFILE%\.cargo\bin;%PATH%"
set "GRAYDB_SOURCE_PORT=5416"
cd /d "%~dp0"
cargo run -p graydb-check --bin demo-sp7
