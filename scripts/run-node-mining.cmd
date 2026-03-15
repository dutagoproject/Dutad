@echo off
setlocal
cd /d "%~dp0.."
set "DATA_DIR=%~1"
if "%DATA_DIR%"=="" set "DATA_DIR=.\data\mainnet"
set "MINING_BIND=%~2"
if "%MINING_BIND%"=="" set "MINING_BIND=127.0.0.1:19085"
cargo run -p dutad -- --datadir "%DATA_DIR%" --mining-bind "%MINING_BIND%"
