# ============================================================
# eventsim_start.ps1
# Chay service eventsim doc lap, ket noi vao kafka dang chay
# ============================================================

$EVENTSIM_IMAGE      = "eventsim:2.0"
$EVENTSIM_CONTAINER  = "eventsim"
$KAFKA_CONTAINER     = "kafka"
$KAFKA_BROKER        = "kafka:9092"

# Lay duong dan thu muc goc cua project (cha cua thu muc scripts/)
$PROJECT_ROOT        = Split-Path -Parent $PSScriptRoot
$DATA_DIR            = Join-Path $PROJECT_ROOT "data"

# Helper
function Write-Info  { param([string]$msg) Write-Host "[INFO]  $msg" -ForegroundColor Cyan    }
function Write-Ok    { param([string]$msg) Write-Host "[OK]    $msg" -ForegroundColor Green   }
function Write-Warn  { param([string]$msg) Write-Host "[WARN]  $msg" -ForegroundColor Yellow  }
function Write-Err   { param([string]$msg) Write-Host "[ERROR] $msg" -ForegroundColor Red     }

# 1. Kiem tra kafka dang chay
Write-Info "Kiem tra container kafka..."
$kafkaId = docker ps --filter "name=^${KAFKA_CONTAINER}$" --filter "status=running" -q 2>$null
if (-not $kafkaId) {
    Write-Err "Container '$KAFKA_CONTAINER' chua chay. Hay khoi dong kafka truoc."
    Write-Warn "Goi y: docker compose up -d kafka"
    exit 1
}
Write-Ok "Kafka dang chay (ID: $kafkaId)"

# 2. Lay network ma kafka dang dung
Write-Info "Lay Docker network cua kafka..."
$kafkaNetwork = docker inspect $KAFKA_CONTAINER --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>$null
if (-not $kafkaNetwork) {
    Write-Err "Khong lay duoc network cua kafka."
    exit 1
}
Write-Ok "Network: $kafkaNetwork"

# 3. Dung & xoa container eventsim cu neu con ton tai
$existing = docker ps -a --filter "name=^${EVENTSIM_CONTAINER}$" -q 2>$null
if ($existing) {
    Write-Warn "Tim thay container '$EVENTSIM_CONTAINER' cu, tien hanh xoa..."
    docker rm -f $EVENTSIM_CONTAINER | Out-Null
    Write-Ok "Da xoa container cu."
}

# 4. Dam bao thu muc data ton tai
if (-not (Test-Path $DATA_DIR)) {
    Write-Info "Tao thu muc data tai: $DATA_DIR"
    New-Item -ItemType Directory -Path $DATA_DIR | Out-Null
}

# 5. Chay eventsim
Write-Info "Khoi dong eventsim..."
Write-Info "  Image   : $EVENTSIM_IMAGE"
Write-Info "  Network : $kafkaNetwork"
Write-Info "  Volume  : $DATA_DIR -> /opt/eventsim/output_data"

docker run -d `
    --name        $EVENTSIM_CONTAINER `
    --network     $kafkaNetwork `
    --volume      "${DATA_DIR}:/opt/eventsim/output_data" `
    $EVENTSIM_IMAGE `
    -c                "examples/config.json" `
    --start-time      "2026-09-01T00:00:00" `
    -n                "100" `
    --growth-rate     "0" `
    --userid          "1" `
    --randomseed      "1" `
    --continuous `
    --kafkaBrokerList $KAFKA_BROKER

if ($LASTEXITCODE -ne 0) {
    Write-Err "Khong the khoi dong eventsim. Xem log phia tren de biet chi tiet."
    exit 1
}

Write-Ok "Eventsim da khoi dong thanh cong!"
Write-Info "Xem log: docker logs -f $EVENTSIM_CONTAINER"
