# Build and run Eventsim independently of the infrastructure Compose project.
$ErrorActionPreference = 'Stop'
$env:EVENTSIM_CONTAINER = 'eventsim'

$eventsimImage = 'img_eventsim'
$eventsimContainer = 'eventsim'
$kafkaBootstrap = 'kafka-1:9092,kafka-2:9092,kafka-3:9092'
$kafkaNetwork = 'eventsim-net'
$projectRoot = Split-Path -Parent $PSScriptRoot
$dataDirectory = Join-Path $projectRoot 'data'
$todayUtc = [DateTime]::UtcNow.Date
$startTime = if ($env:EVENTSIM_START_TIME) { $env:EVENTSIM_START_TIME } else { $todayUtc.ToString('yyyy-MM-ddTHH:mm:ss') }
$endTime = if ($env:EVENTSIM_END_TIME) { $env:EVENTSIM_END_TIME } else { $todayUtc.AddDays(1).ToString('yyyy-MM-ddTHH:mm:ss') }
$utcStyle = [Globalization.DateTimeStyles]::AssumeUniversal -bor [Globalization.DateTimeStyles]::AdjustToUniversal
$startUtc = [DateTime]::ParseExact($startTime, 'yyyy-MM-ddTHH:mm:ss', [Globalization.CultureInfo]::InvariantCulture, $utcStyle)
$endUtc = [DateTime]::ParseExact($endTime, 'yyyy-MM-ddTHH:mm:ss', [Globalization.CultureInfo]::InvariantCulture, $utcStyle)
if ($endUtc -le $startUtc) { throw 'EVENTSIM_END_TIME must be after EVENTSIM_START_TIME (UTC).' }
foreach ($relativePath in @('config/config.json', 'target/eventsim-assembly-2.0.jar')) {
    if (-not (Test-Path -LiteralPath (Join-Path $PSScriptRoot $relativePath) -PathType Leaf)) {
        throw "Missing Eventsim file: $relativePath"
    }
}
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
& docker build --tag $eventsimImage $PSScriptRoot
if ($LASTEXITCODE -ne 0) { throw 'Cannot build Eventsim image.' }
& docker run -d --name $eventsimContainer --network $kafkaNetwork `
    --env TZ=UTC --volume "${dataDirectory}:/opt/eventsim/output_data" $eventsimImage `
    -c 'config/config.json' --start-time $startTime --end-time $endTime `
    --nusers '20000' --nocontinuous --kafkaBrokerList $kafkaBootstrap `
    '/opt/eventsim/output_data'
if ($LASTEXITCODE -ne 0) { throw 'Cannot start Eventsim. See Docker error above.' }
Start-Sleep -Seconds 1
$runtime = & docker inspect $eventsimContainer --format '{{.State.Status}} {{.State.ExitCode}}'
if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect Eventsim runtime.' }
if ($runtime -notin @('running 0', 'exited 0')) {
    & docker logs --tail 30 $eventsimContainer
    throw "Eventsim failed to start: $runtime"
}
Write-Host "Eventsim: $eventsimContainer; state: $runtime; bootstrap: $kafkaBootstrap"
Write-Host "Simulation interval (UTC): $startTime -> $endTime; users: 20000"
Write-Host "Inspect delivery/runtime errors with: docker logs -f $eventsimContainer"
