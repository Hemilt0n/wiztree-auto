param(
    [string]$ConfigPath = (Join-Path (Split-Path -Path $PSScriptRoot -Parent) 'config.json')
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if (-not (Test-Path $ConfigPath)) {
    throw "Config file not found: $ConfigPath"
}

$repoRoot = Split-Path -Path $ConfigPath -Parent
$config = Get-Content -Path $ConfigPath -Raw | ConvertFrom-Json
$taskName = [string]$config.task_name
$scheduleTime = [datetime]::ParseExact([string]$config.schedule_time, 'HH:mm', $null)
$userId = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$powershellExe = (Get-Command powershell.exe).Source
$runDailyScript = Join-Path $repoRoot 'scripts\run_daily.ps1'
$actionArgs = "-NoProfile -ExecutionPolicy Bypass -File `"$runDailyScript`""

$action = New-ScheduledTaskAction -Execute $powershellExe -Argument $actionArgs -WorkingDirectory $repoRoot
$trigger = New-ScheduledTaskTrigger -Daily -At $scheduleTime
$principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType Interactive -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 2)
$task = New-ScheduledTask -Action $action -Trigger $trigger -Principal $principal -Settings $settings

Register-ScheduledTask -TaskName $taskName -InputObject $task -Force | Out-Null

Get-ScheduledTask -TaskName $taskName |
    Select-Object TaskName, State, @{Name = 'RunAs'; Expression = { $_.Principal.UserId } }, @{Name = 'StartBoundary'; Expression = { $_.Triggers[0].StartBoundary } } |
    Format-List |
    Out-String |
    Write-Output

