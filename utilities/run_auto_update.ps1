$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $repoRoot 'logs'
$lockDir = Join-Path $repoRoot 'tmp'
$lockPath = Join-Path $lockDir 'auto_update.lock'
$logPath = Join-Path $logDir 'auto_update.log'
$venvPython = Join-Path $repoRoot '.venv\Scripts\python.exe'
$lockStaleAfterMinutes = 15

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

if (-not (Test-Path $lockDir)) {
    New-Item -ItemType Directory -Path $lockDir | Out-Null
}

function Write-AutoUpdateLog {
    param(
        [string]$Message
    )

    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    Add-Content -Path $logPath -Value "[$timestamp] $Message"
}

function Test-StaleLock {
    param(
        [string]$Path
    )

    try {
        $lockItem = Get-Item -Path $Path
        if ($lockItem.LastWriteTime -lt (Get-Date).AddMinutes(-$lockStaleAfterMinutes)) {
            return $true
        }

        $rawPid = (Get-Content -Path $Path -Raw -ErrorAction SilentlyContinue).Trim()
        if ($rawPid -match '^\d+$') {
            $lockProcess = Get-Process -Id ([int]$rawPid) -ErrorAction SilentlyContinue
            return $null -eq $lockProcess
        }

        return $false
    }
    catch {
        return $false
    }
}

if (Test-Path $lockPath) {
    if (Test-StaleLock -Path $lockPath) {
        Write-AutoUpdateLog 'stale lock found, taking ownership'
    }
    else {
        Write-AutoUpdateLog 'skip: updater is already running'
        exit 0
    }
}

Set-Content -Path $lockPath -Value $PID

try {
    $pythonExe = if (Test-Path $venvPython) { $venvPython } else { 'python' }
    Write-AutoUpdateLog 'run auto_update.py'
    Push-Location $repoRoot
    $outputPath = Join-Path $lockDir 'auto_update.stdout.log'
    $errorPath = Join-Path $lockDir 'auto_update.stderr.log'
    $process = Start-Process -FilePath $pythonExe -ArgumentList 'utilities/auto_update.py' -WorkingDirectory $repoRoot -NoNewWindow -Wait -PassThru -RedirectStandardOutput $outputPath -RedirectStandardError $errorPath
    Get-Content -Path $outputPath, $errorPath -ErrorAction SilentlyContinue | Add-Content -Path $logPath
    $exitCode = $process.ExitCode
    Pop-Location
    Write-AutoUpdateLog "exit code: $exitCode"
    exit $exitCode
}
finally {
    if (Test-Path $lockPath) {
        Remove-Item -Path $lockPath -Force
    }
}