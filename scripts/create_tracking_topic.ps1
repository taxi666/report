$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$javaRoot = Join-Path $projectRoot "tools\java"
$kafkaHome = Join-Path $projectRoot "tools\kafka_2.13-4.2.0"
$topicName = "tracking_events"
$kafkaClasspath = Join-Path $kafkaHome "libs\*"

$jdkDir = Get-ChildItem -Path $javaRoot -Directory | Where-Object { $_.Name -like "jdk-*" } | Select-Object -First 1
if ($null -eq $jdkDir) {
    throw "No JDK directory found under $javaRoot"
}

$env:JAVA_HOME = $jdkDir.FullName
$javaExe = Join-Path $env:JAVA_HOME "bin\java.exe"

& $javaExe `
    -cp $kafkaClasspath `
    org.apache.kafka.tools.TopicCommand `
    --create `
    --if-not-exists `
    --topic $topicName `
    --bootstrap-server 127.0.0.1:9092 `
    --partitions 3 `
    --replication-factor 1

& $javaExe `
    -cp $kafkaClasspath `
    org.apache.kafka.tools.TopicCommand `
    --describe `
    --topic $topicName `
    --bootstrap-server 127.0.0.1:9092
