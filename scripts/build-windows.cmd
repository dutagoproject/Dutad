@echo off
setlocal
cd /d "%~dp0.."
cargo build --release
if errorlevel 1 exit /b 1
echo.
echo Release binaries are in target\release\
