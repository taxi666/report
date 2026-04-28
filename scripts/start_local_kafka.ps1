$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$javaRoot = Join-Path $projectRoot "tools\java"
$kafkaHome = Join-Path $projectRoot "tools\kafka_2.13-4.2.0"
$configPath = Join-Path $projectRoot "infra\kafka-local.properties"
$clusterIdFile = Join-Path $projectRoot "kafka-data\cluster.id"
$logDir = Join-Path $projectRoot "kafka-data\kraft-combined-logs"
$stdoutLog = Join-Path $projectRoot "kafka.stdout.log"
$stderrLog = Join-Path $projectRoot "kafka.stderr.log"
$pidFile = Join-Path $projectRoot "kafka.pid"
$kafkaClasspath = Join-Path $kafkaHome "libs\*"
$javaExe = $null
$runnerScript = Join-Path $projectRoot "scripts\run_local_kafka.cmd"

if (-not (Test-Path $javaRoot)) {
    throw "Java directory not found: $javaRoot"
}

if (-not (Test-Path $kafkaHome)) {
    throw "Kafka directory not found: $kafkaHome"
}

if (-not (Test-Path $configPath)) {
    throw "Kafka config not found: $configPath"
}

$jdkDir = Get-ChildItem -Path $javaRoot -Directory | Where-Object { $_.Name -like "jdk-*" } | Select-Object -First 1
if ($null -eq $jdkDir) {
    throw "No JDK directory found under $javaRoot"
}

$env:JAVA_HOME = $jdkDir.FullName
$javaExe = Join-Path $env:JAVA_HOME "bin\java.exe"

New-Item -ItemType Directory -Force (Join-Path $projectRoot "kafka-data") | Out-Null
New-Item -ItemType Directory -Force $logDir | Out-Null

$metaProperties = Join-Path $logDir "meta.properties"
$log4jConfig = "file:" + ((Join-Path $kafkaHome "config\log4j2.yaml") -replace "\\", "/")

if (-not (Test-Path $clusterIdFile)) {
    $clusterId = (& $javaExe -cp $kafkaClasspath kafka.tools.StorageTool random-uuid).Trim()
    Set-Content -LiteralPath $clusterIdFile -Value $clusterId -Encoding ascii
}
else {
    $clusterId = (Get-Content $clusterIdFile | Select-Object -First 1).Trim()
}

if (-not (Test-Path $metaProperties)) {
    & $javaExe -cp $kafkaClasspath kafka.tools.StorageTool format -t $clusterId -c $configPath | Out-Null
}

if (Test-Path $pidFile) {
    $existingPid = (Get-Content $pidFile | Select-Object -First 1).Trim()
    if ($existingPid) {
        $existingProcess = Get-Process -Id $existingPid -ErrorAction SilentlyContinue
        if ($existingProcess) {
            Write-Output "Kafka broker already running with PID $existingPid"
            exit 0
        }
    }
}

if (Test-Path $stdoutLog) {
    Remove-Item -LiteralPath $stdoutLog -Force
}

if (Test-Path $stderrLog) {
    Remove-Item -LiteralPath $stderrLog -Force
}

cmd /d /c start "" /b $runnerScript | Out-Null
Start-Sleep -Seconds 5

$process = Get-CimInstance Win32_Process |
    Where-Object {
        $_.Name -eq "java.exe" -and
        $_.CommandLine -like "*kafka.Kafka*" -and
        $_.CommandLine -like "*kafka-local.properties*"
    } |
    Sort-Object ProcessId -Descending |
    Select-Object -First 1

if ($null -eq $process) {
    throw "Kafka broker process did not stay running. Check kafka.stderr.log for details."
}

Set-Content -LiteralPath $pidFile -Value $process.ProcessId -Encoding ascii
Write-Output "Kafka broker started with PID $($process.ProcessId)"
