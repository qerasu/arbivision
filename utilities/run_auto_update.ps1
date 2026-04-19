$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $repoRoot 'logs'
$lockDir = Join-Path $repoRoot 'tmp'
$lockPath = Join-Path $lockDir 'auto_update.lock'
$logPath = Join-Path $logDir 'auto_update.log'
$venvPython = Join-Path $repoRoot '.venv\Scripts\python.exe'

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

if (-not (Test-Path $lockDir)) {
    New-Item -ItemType Directory -Path $lockDir | Out-Null
}

if (Test-Path $lockPath) {
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    Add-Content -Path $logPath -Value "[$timestamp] skip: updater is already running"
    exit 0
}

New-Item -ItemType File -Path $lockPath | Out-Null

try {
    $pythonExe = if (Test-Path $venvPython) { $venvPython } else { 'python' }
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    Add-Content -Path $logPath -Value "[$timestamp] run auto_update.py"
    Push-Location $repoRoot
    $outputPath = Join-Path $lockDir 'auto_update.stdout.log'
    $errorPath = Join-Path $lockDir 'auto_update.stderr.log'
    $process = Start-Process -FilePath $pythonExe -ArgumentList 'utilities/auto_update.py' -WorkingDirectory $repoRoot -NoNewWindow -Wait -PassThru -RedirectStandardOutput $outputPath -RedirectStandardError $errorPath
    Get-Content -Path $outputPath, $errorPath -ErrorAction SilentlyContinue | Add-Content -Path $logPath
    $exitCode = $process.ExitCode
    Pop-Location
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    Add-Content -Path $logPath -Value "[$timestamp] exit code: $exitCode"
    exit $exitCode
}
finally {
    if (Test-Path $lockPath) {
        Remove-Item -Path $lockPath -Force
    }
}
