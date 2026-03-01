# =============================================================================
# DineScores — Register Windows Task Scheduler Job
# =============================================================================
# Schedules run_local_cities.bat to run every Sunday at 4:00 AM.
# Runs AFTER the cloud GitHub Actions job (3:00 AM CST) so Firestore gets
# both cloud + local city data in the same weekly cycle.
#
# USAGE (run once from PowerShell as Administrator):
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#   .\register_task_scheduler.ps1
#
# ADDING NEW CITIES:
#   Edit the $Cities variable below, then re-run this script.
#
# REMOVE THE TASK:
#   Unregister-ScheduledTask -TaskName "DineScores Local Refresh" -Confirm:$false
# =============================================================================

# ── Config — UPDATE THIS to your actual repo path ───────────────────────
$RepoPath  = "$env:USERPROFILE\DineScores"   # e.g. C:\Users\Jordan\DineScores
$BatchFile = "$RepoPath\run_local_cities.bat"
$LogDir    = "$RepoPath\logs"

# WAF-blocked cities — add new ones here as you expand
$Cities = @("dallas")
# Future: $Cities = @("dallas", "houston", "miami")

$TaskName  = "DineScores Local Refresh"
$TaskDesc  = "Weekly refresh of DineScores data for WAF-blocked cities (Dallas, etc.)"

# ── Preflight ──────────────────────────────────────────────────────────────
if (-not (Test-Path $BatchFile)) {
    Write-Error "Batch file not found: $BatchFile`nMake sure you've cloned the DineScores repo to: $RepoPath"
    exit 1
}

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

# ── Build the action ────────────────────────────────────────────────────────
$CityArgs  = ($Cities -join " ")
$Arguments = "/c `"$BatchFile`" --mode weekly --cities $CityArgs >> `"$LogDir\task_scheduler.log`" 2>&1"
$Action    = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $Arguments -WorkingDirectory $RepoPath

# ── Schedule: every Sunday at 4:00 AM ──────────────────────────────────
$Trigger   = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At "4:00AM"

# ── Settings ──────────────────────────────────────────────────────────────
$Settings  = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)

# ── Register ──────────────────────────────────────────────────────────────
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive

$Params = @{
    TaskName    = $TaskName
    Description = $TaskDesc
    Action      = $Action
    Trigger     = $Trigger
    Settings    = $Settings
    Principal   = $Principal
    Force       = $true
}

Register-ScheduledTask @Params | Out-Null

Write-Host ""
Write-Host "Task registered successfully!" -ForegroundColor Green
Write-Host "  Name:     $TaskName"
Write-Host "  Cities:   $CityArgs"
Write-Host "  Schedule: Every Sunday at 4:00 AM"
Write-Host "  Log:      $LogDir\task_scheduler.log"
Write-Host ""
Write-Host "To run manually right now:"
Write-Host "  Start-ScheduledTask -TaskName '$TaskName'"
Write-Host ""
Write-Host "To view in Task Scheduler UI: taskschd.msc"
