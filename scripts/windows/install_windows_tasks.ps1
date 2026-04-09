param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"

$ProjectRoot = "E:\a-stock-advisor"
$Runner = Join-Path $ProjectRoot "scripts\windows\run_task.ps1"
$PowerShellExe = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"

if (-not (Test-Path $Runner)) {
    throw "Task runner not found: $Runner"
}

$Tasks = @(
    @{ Name = "A-Stock-Advisor Morning Master"; Schedule = "WEEKLY"; Days = "MON,TUE,WED,THU,FRI"; Time = "08:00"; TaskKey = "morning_master_push" },
    @{ Name = "A-Stock-Advisor Evening Master"; Schedule = "WEEKLY"; Days = "MON,TUE,WED,THU,FRI"; Time = "18:30"; TaskKey = "daily_selection_push" },
    @{ Name = "A-Stock-Advisor Health Check"; Schedule = "DAILY"; Time = "03:00"; TaskKey = "health_check" },
    @{ Name = "A-Stock-Advisor Monitor Collector"; Schedule = "HOURLY"; Modifier = 1; TaskKey = "monitor_collector" },
    @{ Name = "A-Stock-Advisor Weekly Backtest"; Schedule = "WEEKLY"; Days = "SUN"; Time = "02:00"; TaskKey = "weekly_backtest" }
)

foreach ($task in $Tasks) {
    $args = @(
        "/Create",
        "/TN", $task.Name,
        "/TR", "`"$PowerShellExe`" -NoProfile -ExecutionPolicy Bypass -File `"$Runner`" -TaskName $($task.TaskKey)",
        "/SC", $task.Schedule,
        "/RL", "LIMITED",
        "/F"
    )

    if ($task.ContainsKey("Days")) { $args += @("/D", $task.Days) }
    if ($task.ContainsKey("Time")) { $args += @("/ST", $task.Time) }
    if ($task.ContainsKey("Modifier")) { $args += @("/MO", [string]$task.Modifier) }

    & schtasks.exe @args | Out-Null
}

Write-Output "windows-tasks-installed"
