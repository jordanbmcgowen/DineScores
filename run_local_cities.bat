@echo off
:: =============================================================================
:: DineScores Local Runner — Windows
:: =============================================================================
:: Runs the pipeline for WAF-protected cities (Dallas, and future cities that
:: block cloud IPs). Pushes data directly to Firestore from your local machine.
::
:: Usage (from Command Prompt or PowerShell):
::   run_local_cities.bat                          — weekly refresh, all local cities
::   run_local_cities.bat --cities dallas houston   — specific cities
::   run_local_cities.bat --mode full               — full historical pull
::   run_local_cities.bat --dry-run                 — test without writing to Firestore
::
:: Setup (first time only):
::   1. pip install firebase-admin requests
::   2. Download Firebase service account key:
::      Firebase Console -> Project Settings -> Service Accounts -> Generate new private key
::      Save as: %USERPROFILE%\.dinescores\firebase-key.json
:: =============================================================================

setlocal EnableDelayedExpansion

:: ── Config ─────────────────────────────────────────────────────────────
set SCRIPT_DIR=%~dp0
set PIPELINE=%SCRIPT_DIR%dinescores_pipeline.py
set CREDS=%USERPROFILE%\.dinescores\firebase-key.json
set LOG_DIR=%SCRIPT_DIR%logs

:: Default WAF-blocked cities — add new ones here as you expand
set LOCAL_CITIES=dallas

set MODE=weekly
set DRY_RUN=

:: ── Argument parsing ─────────────────────────────────────────────────────────
:parse_args
if "%~1"=="" goto preflight
if "%~1"=="--mode" (
    set MODE=%~2
    shift & shift & goto parse_args
)
if "%~1"=="--cities" (
    set LOCAL_CITIES=
    shift
    :cities_loop
    if "%~1"=="" goto preflight
    if "%~1:~0,2%"=="--" goto preflight
    if defined LOCAL_CITIES (
        set LOCAL_CITIES=!LOCAL_CITIES! %~1
    ) else (
        set LOCAL_CITIES=%~1
    )
    shift & goto cities_loop
)
if "%~1"=="--dry-run" (
    set DRY_RUN=--dry-run
    shift & goto parse_args
)
shift & goto parse_args

:: ── Preflight checks ─────────────────────────────────────────────────────────
:preflight
echo ========================================
echo   DineScores Local Runner
echo   %date% %time%
echo ========================================

if not exist "%PIPELINE%" (
    echo ERROR: Pipeline not found: %PIPELINE%
    exit /b 1
)

if not exist "%CREDS%" (
    echo ERROR: Firebase credentials not found: %CREDS%
    echo.
    echo To fix:
    echo   1. Go to: https://console.firebase.google.com/project/healthinspections/settings/serviceaccounts/adminsdk
    echo   2. Click "Generate new private key"
    echo   3. Save the file as: %CREDS%
    echo   (or set DINESCORES_FIREBASE_CREDS=C:\path\to\key.json^)
    exit /b 1
)

if defined DINESCORES_FIREBASE_CREDS set CREDS=%DINESCORES_FIREBASE_CREDS%

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

:: Generate timestamped log filename
for /f "tokens=1-3 delims=/ " %%a in ("%date%") do set DATESTAMP=%%c%%a%%b
for /f "tokens=1-2 delims=:." %%a in ("%time: =0%") do set TIMESTAMP=%%a%%b
set LOG_FILE=%LOG_DIR%\local_run_%DATESTAMP%_%TIMESTAMP%.log

echo   Mode:    %MODE%
echo   Cities:  %LOCAL_CITIES%
echo   Creds:   %CREDS%
echo   Log:     %LOG_FILE%
if defined DRY_RUN echo   WARNING: DRY RUN -- no Firestore writes
echo.

:: ── Run pipeline ─────────────────────────────────────────────────────────────
python "%PIPELINE%" ^
    --mode %MODE% ^
    --cities %LOCAL_CITIES% ^
    --creds "%CREDS%" ^
    %DRY_RUN% ^
    2>&1 | tee "%LOG_FILE%"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Done! Log saved to: %LOG_FILE%
) else (
    echo.
    echo ERROR: Pipeline failed. Check log: %LOG_FILE%
    exit /b %ERRORLEVEL%
)
