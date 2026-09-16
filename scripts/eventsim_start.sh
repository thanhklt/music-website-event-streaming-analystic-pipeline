#!/bin/bash
# ============================================================
# eventsim_start.sh
# Chay service eventsim doc lap, ket noi vao kafka dang chay
# ============================================================

set -euo pipefail

EVENTSIM_IMAGE="eventsim:2.0"
EVENTSIM_CONTAINER="eventsim"
KAFKA_CONTAINER="kafka"
KAFKA_BROKER="kafka:9092"

# Lay duong dan thu muc goc cua project (cha cua thu muc scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_ROOT/data"

# Helper
info()  { echo -e "\033[36m[INFO]  $1\033[0m"; }
ok()    { echo -e "\033[32m[OK]    $1\033[0m"; }
warn()  { echo -e "\033[33m[WARN]  $1\033[0m"; }
err()   { echo -e "\033[31m[ERROR] $1\033[0m"; }

# 1. Kiem tra kafka dang chay
info "Kiem tra container kafka..."
KAFKA_ID=$(docker ps --filter "name=^${KAFKA_CONTAINER}$" --filter "status=running" -q 2>/dev/null || true)
if [ -z "$KAFKA_ID" ]; then
    err "Container '$KAFKA_CONTAINER' chua chay. Hay khoi dong kafka truoc."
    warn "Goi y: docker compose up -d kafka"
    exit 1
fi
ok "Kafka dang chay (ID: $KAFKA_ID)"

# 2. Lay network ma kafka dang dung
info "Lay Docker network cua kafka..."
KAFKA_NETWORK=$(docker inspect "$KAFKA_CONTAINER" \
    --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>/dev/null || true)
if [ -z "$KAFKA_NETWORK" ]; then
    err "Khong lay duoc network cua kafka."
    exit 1
fi
ok "Network: $KAFKA_NETWORK"

# 3. Dung & xoa container eventsim cu neu con ton tai
EXISTING=$(docker ps -a --filter "name=^${EVENTSIM_CONTAINER}$" -q 2>/dev/null || true)
if [ -n "$EXISTING" ]; then
    warn "Tim thay container '$EVENTSIM_CONTAINER' cu, tien hanh xoa..."
    docker rm -f "$EVENTSIM_CONTAINER" > /dev/null
    ok "Da xoa container cu."
fi

# 4. Dam bao thu muc data ton tai
if [ ! -d "$DATA_DIR" ]; then
    info "Tao thu muc data tai: $DATA_DIR"
    mkdir -p "$DATA_DIR"
fi

# 5. Chay eventsim
info "Khoi dong eventsim..."
info "  Image   : $EVENTSIM_IMAGE"
info "  Network : $KAFKA_NETWORK"
info "  Volume  : $DATA_DIR -> /opt/eventsim/output_data"

docker run -d \
    --name        "$EVENTSIM_CONTAINER" \
    --network     "$KAFKA_NETWORK" \
    --volume      "$DATA_DIR:/opt/eventsim/output_data" \
    "$EVENTSIM_IMAGE" \
    -c                "examples/config.json" \
    --start-time      "2026-09-01T00:00:00" \
    -n                "100" \
    --growth-rate     "0" \
    --userid          "1" \
    --randomseed      "1" \
    --continuous \
    --kafkaBrokerList "$KAFKA_BROKER"

ok "Eventsim da khoi dong thanh cong!"
info "Xem log: docker logs -f $EVENTSIM_CONTAINER"