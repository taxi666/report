$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$bundledPython = Join-Path $env:USERPROFILE ".cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
$venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"

if (Test-Path $bundledPython) {
    $pythonExe = $bundledPython
}
elseif (Test-Path $venvPython) {
    $pythonExe = $venvPython
}
else {
    throw "Python runtime not found."
}

Push-Location $projectRoot
try {
    & $pythonExe -m uvicorn app.main:app --host 0.0.0.0 --port 8000
}
finally {
    Pop-Location
}
