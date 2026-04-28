$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$bundledPython = "C:\Users\祝\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
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
    & $pythonExe -m uvicorn app.main:app --host 127.0.0.1 --port 8000
}
finally {
    Pop-Location
}
