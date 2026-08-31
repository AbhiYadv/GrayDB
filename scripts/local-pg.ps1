# local-pg.ps1 — no-Docker fallback for the dev sources (Decision D-005).
# Runs portable PostgreSQL 16 + 17 binaries as user-scope local processes with the
# same contract as docker-compose.yml: wal_level=logical, pg16 -> 5416, pg17 -> 5417,
# db appdb, user postgres, password graydb (auth scram-sha-256 — exercises our SCRAM path).
#
# Usage:  .\scripts\local-pg.ps1 -Action init|start|stop|status [-Version 16|17|both]
param(
    [Parameter(Mandatory = $true)][ValidateSet('init', 'start', 'stop', 'status')][string]$Action,
    [ValidateSet('16', '17', 'both')][string]$Version = 'both'
)

$ErrorActionPreference = 'Stop'
$ToolsDir = Join-Path $PSScriptRoot '..\..\tools' | Resolve-Path -ErrorAction SilentlyContinue
if (-not $ToolsDir) { $ToolsDir = Join-Path (Split-Path (Split-Path $PSScriptRoot)) 'tools' }

$Instances = @{
    '16' = @{ Bin = Join-Path $ToolsDir 'pg16\pgsql\bin'; Data = Join-Path $ToolsDir 'pgdata\pg16'; Port = 5416 }
    '17' = @{ Bin = Join-Path $ToolsDir 'pg17\pgsql\bin'; Data = Join-Path $ToolsDir 'pgdata\pg17'; Port = 5417 }
}
$Selected = if ($Version -eq 'both') { @('16', '17') } else { @($Version) }

foreach ($v in $Selected) {
    $i = $Instances[$v]
    $initdb = Join-Path $i.Bin 'initdb.exe'
    $pgctl  = Join-Path $i.Bin 'pg_ctl.exe'
    $psql   = Join-Path $i.Bin 'psql.exe'
    $log    = Join-Path $i.Data 'server.log'

    switch ($Action) {
        'init' {
            if (Test-Path (Join-Path $i.Data 'PG_VERSION')) {
                Write-Output "pg$v : already initialized at $($i.Data)"
                break
            }
            New-Item -ItemType Directory -Force (Split-Path $i.Data) | Out-Null
            $pw = Join-Path $env:TEMP "graydb-pg-pw.txt"
            Set-Content -Path $pw -Value 'graydb' -Encoding ascii -NoNewline
            & $initdb -D $i.Data -U postgres -E UTF8 -A scram-sha-256 --pwfile=$pw
            if ($LASTEXITCODE -ne 0) { throw "initdb failed for pg$v" }
            Remove-Item $pw -Force
            Add-Content -Path (Join-Path $i.Data 'postgresql.conf') -Encoding ascii -Value @"

# --- graydb dev source (mirrors docker-compose.yml) ---
listen_addresses = '127.0.0.1'
port = $($i.Port)
wal_level = logical
max_replication_slots = 8
max_wal_senders = 8
"@
            Write-Output "pg$v : initialized (port $($i.Port), wal_level=logical)"
        }
        'start' {
            & $pgctl -D $i.Data -l $log -w start
            if ($LASTEXITCODE -ne 0) { throw "pg_ctl start failed for pg$v (see $log)" }
            # Create appdb if missing (idempotent).
            $env:PGPASSWORD = 'graydb'
            $exists = & $psql -h 127.0.0.1 -p $i.Port -U postgres -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='appdb'"
            if ($exists -ne '1') {
                & $psql -h 127.0.0.1 -p $i.Port -U postgres -d postgres -c 'CREATE DATABASE appdb' | Out-Null
            }
            Remove-Item Env:\PGPASSWORD
            Write-Output "pg$v : running on 127.0.0.1:$($i.Port) (db appdb ready)"
        }
        'stop' {
            & $pgctl -D $i.Data -m fast -w stop
            Write-Output "pg$v : stopped"
        }
        'status' {
            & $pgctl -D $i.Data status
        }
    }
}
