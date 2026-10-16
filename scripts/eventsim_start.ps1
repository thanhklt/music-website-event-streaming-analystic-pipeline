# Eventsim remains independent of Compose; broker healthchecks call the Kafka API.
$ErrorActionPreference = 'Stop'
$eventsimImage = if ($env:EVENTSIM_IMAGE) { $env:EVENTSIM_IMAGE } else { 'eventsim:2.0' }
$eventsimContainer = if ($env:EVENTSIM_CONTAINER) { $env:EVENTSIM_CONTAINER } else { 'eventsim-v2' }
$kafkaBootstrap = if ($env:EVENTSIM_KAFKA_BOOTSTRAP_SERVERS) { $env:EVENTSIM_KAFKA_BOOTSTRAP_SERVERS } else { 'kafka-1:9092,kafka-2:9092,kafka-3:9092' }
$kafkaNetwork = if ($env:EVENTSIM_KAFKA_NETWORK) { $env:EVENTSIM_KAFKA_NETWORK } else { 'eventsim-net' }
$projectRoot = Split-Path -Parent $PSScriptRoot
$dataDirectory = Join-Path $projectRoot 'data'
& docker ps --format '{{.Names}}' | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Docker daemon is unavailable.' }
$existing = & docker ps -a --filter "name=^${eventsimContainer}$" --format '{{.Names}}'
if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect Eventsim container.' }
if ($existing) { throw "Container '$eventsimContainer' exists and was preserved. Choose a new EVENTSIM_CONTAINER or explicitly stop/start it." }
$deadline = [DateTime]::UtcNow.AddSeconds(60)
do {
    $ready = $true
    foreach ($broker in @('kafka-1', 'kafka-2', 'kafka-3')) {
        $health = & docker inspect $broker --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}'
        if ($LASTEXITCODE -ne 0 -or $health -ne 'healthy') { $ready = $false }
    }
    if ($ready) { break }
    if ([DateTime]::UtcNow -ge $deadline) { throw 'Kafka API healthchecks did not become healthy within 60s. Start infra/docker-compose.yml first.' }
    Start-Sleep -Seconds 2
} while ($true)
& docker network inspect $kafkaNetwork --format '{{.Name}}' | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Docker network '$kafkaNetwork' is unavailable." }
New-Item -ItemType Directory -Path $dataDirectory -Force | Out-Null
& docker run -d --name $eventsimContainer --network $kafkaNetwork `
    --volume "${dataDirectory}:/opt/eventsim/output_data" $eventsimImage `
    -c 'examples/config.json' --start-time '2026-09-01T00:00:00' `
    -n '100' --growth-rate '0' --userid '1' --randomseed '1' --nocontinuous `
    --kafkaBrokerList $kafkaBootstrap
if ($LASTEXITCODE -ne 0) { throw 'Cannot start Eventsim. See Docker error above.' }
Write-Host "Eventsim started: $eventsimContainer; bootstrap: $kafkaBootstrap"
Write-Host "Inspect delivery/runtime errors with: docker logs -f $eventsimContainer"
