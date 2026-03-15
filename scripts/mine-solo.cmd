@echo off
setlocal
cd /d "%~dp0.."
set "RPC=%~1"
if "%RPC%"=="" set "RPC=http://127.0.0.1:19085"
set "ADDRESS=%~2"
if "%ADDRESS%"=="" set "ADDRESS=YOUR_DUTA_ADDRESS"
set "THREADS=%~3"
if "%THREADS%"=="" set "THREADS=8"
cargo run -p dutad --bin dutaminer -- --rpc "%RPC%" --address "%ADDRESS%" --threads "%THREADS%"
