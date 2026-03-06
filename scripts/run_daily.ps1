param(
    [switch]$Force
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$logDir = Join-Path $repoRoot 'data\logs'
$python = Join-Path $repoRoot '.venv\Scripts\python.exe'
$script = Join-Path $repoRoot 'scripts\run_pipeline.py'
$config = Join-Path $repoRoot 'config.json'
$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$logPath = Join-Path $logDir "run_$timestamp.log"

New-Item -ItemType Directory -Path $logDir -Force | Out-Null
Set-Location $repoRoot

if (-not (Test-Path $python)) {
    throw "Virtual environment Python was not found: $python"
}

$args = @(
    $script,
    '--config',
    $config
)

if ($Force) {
    $args += '--force'
}

"[$(Get-Date -Format s)] Starting WizTree pipeline" | Set-Content -Path $logPath -Encoding UTF8
"RepoRoot=$repoRoot" | Add-Content -Path $logPath -Encoding UTF8
"Command=$python $($args -join ' ')" | Add-Content -Path $logPath -Encoding UTF8

& $python @args 2>&1 | Tee-Object -FilePath $logPath -Append
$exitCode = $LASTEXITCODE

"[$(Get-Date -Format s)] ExitCode=$exitCode" | Add-Content -Path $logPath -Encoding UTF8
exit $exitCode
