@echo off
rem Build release binaries and assemble a portable GrayDB folder in dist\.
rem The result runs from any directory on any Windows x64 machine with no Rust,
rem no PostgreSQL client libraries, and no installer.
setlocal
set "PATH=%USERPROFILE%\.cargo\bin;%~dp0..\tools\llvm-mingw-20260616-ucrt-x86_64\bin;%PATH%"
cd /d "%~dp0"

echo [1/3] building release binaries (thin LTO, this takes a while) ...
cargo build --release --workspace || exit /b 1

set "OUT=dist\graydb-windows-x64"
set "REL=target\x86_64-pc-windows-gnullvm\release"

echo [2/3] assembling %OUT% ...
if exist "%OUT%" rmdir /s /q "%OUT%"
mkdir "%OUT%\docs" 2>nul
copy /y "%REL%\graydb-studio.exe" "%OUT%\" >nul || exit /b 1
for %%D in (1 2 3 4 5 6 7) do copy /y "%REL%\demo-sp%%D.exe" "%OUT%\" >nul
copy /y "graydb.toml" "%OUT%\" >nul
copy /y "db\seed.sql" "%OUT%\" >nul
copy /y "docs\SETUP.md" "%OUT%\docs\" >nul
copy /y "docs\DEMO.md" "%OUT%\docs\" >nul
copy /y "docs\MILESTONES.md" "%OUT%\docs\" >nul
copy /y "docs\DECISIONS.md" "%OUT%\docs\" >nul
> "%OUT%\run-studio.cmd" echo @echo off
>> "%OUT%\run-studio.cmd" echo rem GrayDB Studio - edit graydb.toml first to point at your PostgreSQL.
>> "%OUT%\run-studio.cmd" echo cd /d "%%~dp0"
>> "%OUT%\run-studio.cmd" echo graydb-studio.exe

echo [3/3] done. Contents:
dir /b "%OUT%"
echo.
echo Zip %OUT% and it runs on any Windows x64 box:
echo   1. edit graydb.toml  ([source] host/port/dbname/user/password/schema)
echo   2. run-studio.cmd    then open http://127.0.0.1:7432
endlocal
