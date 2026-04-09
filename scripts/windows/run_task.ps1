param(
    [Parameter(Mandatory = $true)]
    [string]$TaskName
)

$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\.." )).Path
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$LogsDir = Join-Path $ProjectRoot "logs"

New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

if (-not (Test-Path $PythonExe)) {
    throw "Python virtual environment not found: $PythonExe"
}

$TaskMap = @{
    "morning_master_push" = @{ Command = @("scripts\daily_push.py", "--mode", "morning"); Log = "task_morning_master.log" }
    "daily_selection_push" = @{ Command = @("scripts\daily_push.py", "--mode", "evening"); Log = "task_daily_selection.log" }
    "health_check" = @{ Command = @("scripts\health_check.py"); Log = "task_health_check.log" }
    "monitor_collector" = @{ Command = @("scripts\monitor_collector.py"); Log = "task_monitor_collector.log" }
    "weekly_backtest" = @{ Command = @("scripts\run_backtest.py"); Log = "task_backtest.log" }
}

if (-not $TaskMap.ContainsKey($TaskName)) {
    throw "Unknown task: $TaskName"
}

$Spec = $TaskMap[$TaskName]
$LogPath = Join-Path $LogsDir $Spec.Log

Push-Location $ProjectRoot
try {
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & $PythonExe @($Spec.Command) *>> $LogPath
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousErrorActionPreference
    exit $exitCode
}
finally {
    Pop-Location
}
