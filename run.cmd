@echo off
setlocal

set "ROOT_DIR=%~dp0"

where py >nul 2>nul
if %errorlevel%==0 (
    py -3 "%ROOT_DIR%run.py" %*
    exit /b %errorlevel%
)

where python >nul 2>nul
if %errorlevel%==0 (
    python "%ROOT_DIR%run.py" %*
    exit /b %errorlevel%
)

echo Python 3.9+ is required but was not found on PATH.
exit /b 1
