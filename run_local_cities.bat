@echo off
:: =============================================================================
:: DineScores Local Runner — Windows
:: =============================================================================
:: Usage:
::   run_local_cities.bat                          -- weekly refresh (dallas)
::   run_local_cities.bat full                     -- full historical pull
::   run_local_cities.bat full dallas houston       -- full pull, specific cities
::   run_local_cities.bat weekly dallas             -- weekly, specific cities
:: =============================================================================

setlocal EnableDelayedExpansion

:: ── Config ─────────────────────────────────────────────────────────────
set SCRIPT_DIR=%~dp0
set PIPELINE=%SCRIPT_DIR%dinescores_pipeline.py
set CREDS=%USERPROFILE%\.dinescores\firebase-key.json
set LOG_DIR=%SCRIPT_DIR%logs

:: First argument = mode (optional, defaults to weekly)
:: Remaining arguments = cities (optional, defaults to dallas)
set MODE=weekly
set LOCAL_CITIES=dallas

if not "%~1"=="" (
    set MODE=%~1
)
if not "%~2"=="" (
    set LOCAL_CITIES=%~2
)
if not "%~3"=="" (
    set LOCAL_CITIES=%LOCAL_CITIES% %~3
)
if not "%~4"=="" (
    set LOCAL_CITIES=%LOCAL_CITIES% %~4
)
if not "%~5"=="" (
    set LOCAL_CITIES=%LOCAL_CITIES% %~5
)

:: ── Preflight checks ─────────────────────────────────────────────────────────
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
set LOG_FILE=%LOG_DIR%\local_run_%date:~-4,4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~3,2%.log
set LOG_FILE=%LOG_FILE: =0%

echo   Mode:    %MODE%
echo   Cities:  %LOCAL_CITIES%
echo   Creds:   %CREDS%
echo   Log:     %LOG_FILE%
echo.

:: ── Run pipeline ─────────────────────────────────────────────────────────────
python "%PIPELINE%" --mode %MODE% --cities %LOCAL_CITIES% --creds "%CREDS%" > "%LOG_FILE%" 2>&1

set EXIT_CODE=%ERRORLEVEL%
type "%LOG_FILE%"

echo.
if %EXIT_CODE% EQU 0 (
    echo Done! Log saved to: %LOG_FILE%
) else (
    echo ERROR: Pipeline failed. Check log: %LOG_FILE%
)

exit /b %EXIT_CODE%
