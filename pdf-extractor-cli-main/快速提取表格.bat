@echo off
chcp 65001 >nul
echo ========================================
echo   Quick Table Extraction Mode
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo [Error] Python not found, please install Python 3.11+
    pause
    exit /b 1
)

REM Run quick mode
python start.py --quick

if errorlevel 1 (
    echo.
    echo [Error] Program execution failed
    pause
    exit /b 1
)

pause


