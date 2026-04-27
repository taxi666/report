$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $pythonExe)) {
    throw "Python runtime not found: $pythonExe"
}

Push-Location $projectRoot
try {
    & $pythonExe -m uvicorn app.main:app --host 127.0.0.1 --port 8000
}
finally {
    Pop-Location
}
