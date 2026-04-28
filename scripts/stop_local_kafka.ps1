$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$pidFile = Join-Path $projectRoot "kafka.pid"

if (-not (Test-Path $pidFile)) {
    Write-Output "Kafka broker PID file not found."
    exit 0
}

$pid = (Get-Content $pidFile | Select-Object -First 1).Trim()
if (-not $pid) {
    Remove-Item -LiteralPath $pidFile -Force
    Write-Output "Kafka broker PID file was empty."
    exit 0
}

$process = Get-Process -Id $pid -ErrorAction SilentlyContinue
if ($process) {
    Stop-Process -Id $pid -Force
    Write-Output "Kafka broker stopped: PID $pid"
}
else {
    Write-Output "Kafka broker process not found for PID $pid"
}

Remove-Item -LiteralPath $pidFile -Force
