@echo off
:: =============================================================================
:: DineScores Local Runner — Windows
:: =============================================================================
:: Usage:
::   run_local_cities.bat                          -- weekly refresh, all local cities
::   run_local_cities.bat --cities dallas           -- specific cities
::   run_local_cities.bat --mode full               -- full historical pull
::   run_local_cities.bat --dry-run                 -- test without writing to Firestore
:: =============================================================================

setlocal EnableDelayedExpansion

:: ── Config ─────────────────────────────────────────────────────────────
set SCRIPT_DIR=%~dp0
set PIPELINE=%SCRIPT_DIR%dinescores_pipeline.py
set CREDS=%USERPROFILE%\.dinescores\firebase-key.json
set LOG_DIR=%SCRIPT_DIR%logs

set LOCAL_CITIES=dallas
set MODE=weekly
set DRY_RUN=

:: ── Argument parsing ─────────────────────────────────────────────────────────
:parse_args
if "%~1"=="" goto preflight
if /i "%~1"=="--mode" (
    set MODE=%~2
    shift & shift & goto parse_args
)
if /i "%~1"=="--cities" (
    shift
    set LOCAL_CITIES=
    :cities_loop
    if "%~1"=="" goto preflight
    echo %~1 | findstr /b "--" >nul && goto preflight
    if defined LOCAL_CITIES (
        set "LOCAL_CITIES=!LOCAL_CITIES! %~1"
    ) else (
        set "LOCAL_CITIES=%~1"
    )
    shift & goto cities_loop
)
if /i "%~1"=="--dry-run" (
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

if defined DINESCORES_FIREBASE_CREDS set CREDS=%DINESCORES_FIREBASE_CREDS%

if not exist "%CREDS%" (
    echo ERROR: Firebase credentials not found: %CREDS%
    echo.
    echo To fix:
    echo   1. Go to: https://console.firebase.google.com/project/healthinspections/settings/serviceaccounts/adminsdk
    echo   2. Click "Generate new private key"
    echo   3. Save the file as: %CREDS%
    exit /b 1
)

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set DT=%%I
set LOG_FILE=%LOG_DIR%\local_run_%DT:~0,15%.log

echo   Mode:    %MODE%
echo   Cities:  %LOCAL_CITIES%
echo   Creds:   %CREDS%
echo   Log:     %LOG_FILE%
if defined DRY_RUN echo   WARNING: DRY RUN -- no Firestore writes
echo.

:: ── Run pipeline ─────────────────────────────────────────────────────────────
python "%PIPELINE%" --mode %MODE% --cities %LOCAL_CITIES% --creds "%CREDS%" %DRY_RUN% > "%LOG_FILE%" 2>&1

set EXIT_CODE=%ERRORLEVEL%

:: Also print log to screen
type "%LOG_FILE%"

echo.
if %EXIT_CODE% EQU 0 (
    echo Done! Log saved to: %LOG_FILE%
) else (
    echo ERROR: Pipeline failed ^(exit code %EXIT_CODE%^). Check log: %LOG_FILE%
)

exit /b %EXIT_CODE%
