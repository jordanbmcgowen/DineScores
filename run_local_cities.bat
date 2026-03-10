@echo off
:: =============================================================================
:: DineScores Local Runner — Windows
:: =============================================================================
:: Usage:
::   run_local_cities.bat                    -- weekly refresh, all DFW cities
::   run_local_cities.bat full               -- full historical pull, all DFW
::   run_local_cities.bat full dallas plano  -- full pull, specific cities
::   run_local_cities.bat weekly dallas      -- weekly, specific city
::
:: The default "dfw" expands to all 16 DFW metro cities in the pipeline.
:: Individual city slugs: dallas, fortworth, arlington, plano, irving, frisco,
::   mckinney, denton, garland, grandprairie, mesquite, carrollton,
::   richardson, allen, lewisville, flowermound
:: =============================================================================

setlocal EnableDelayedExpansion

:: ── Config ─────────────────────────────────────────────────────────────
set SCRIPT_DIR=%~dp0
set PIPELINE=%SCRIPT_DIR%dinescores_pipeline.py
set CREDS=%USERPROFILE%\.dinescores\firebase-key.json
set LOG_DIR=%SCRIPT_DIR%logs

:: First argument = mode (optional, defaults to weekly)
:: Remaining arguments = cities (optional, defaults to dfw = all DFW metro)
set MODE=weekly
set LOCAL_CITIES=

if not "%~1"=="" (
    set MODE=%~1
)

:: Collect all remaining arguments as city names
shift
:collect_cities
if "%~1"=="" goto done_cities
if "!LOCAL_CITIES!"=="" (
    set LOCAL_CITIES=%~1
) else (
    set LOCAL_CITIES=!LOCAL_CITIES! %~1
)
shift
goto collect_cities
:done_cities

:: Default to all DFW cities if none specified
if "!LOCAL_CITIES!"=="" set LOCAL_CITIES=dfw

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
echo   Cities:  !LOCAL_CITIES!
echo   Creds:   %CREDS%
echo   Log:     %LOG_FILE%
echo.

:: ── Run pipeline ─────────────────────────────────────────────────────────────
python "%PIPELINE%" --mode %MODE% --cities !LOCAL_CITIES! --creds "%CREDS%" > "%LOG_FILE%" 2>&1

set EXIT_CODE=%ERRORLEVEL%
type "%LOG_FILE%"

echo.
if %EXIT_CODE% EQU 0 (
    echo Done! Log saved to: %LOG_FILE%
) else (
    echo ERROR: Pipeline failed. Check log: %LOG_FILE%
)

exit /b %EXIT_CODE%
