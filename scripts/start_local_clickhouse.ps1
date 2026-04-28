$ErrorActionPreference = "Stop"

$containerName = "tracking-clickhouse"
$imageName = "clickhouse/clickhouse-server:latest"

$existing = docker ps -a --filter "name=$containerName" --format "{{.Names}}"
if ($existing -contains $containerName) {
    docker start $containerName | Out-Null
}
else {
    docker run `
        --name $containerName `
        -d `
        -p 8123:8123 `
        -p 9000:9000 `
        -e CLICKHOUSE_DB=analytics `
        -e CLICKHOUSE_USER=default `
        -e CLICKHOUSE_PASSWORD= `
        -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 `
        $imageName | Out-Null
}

Write-Output "ClickHouse container is starting: $containerName"
