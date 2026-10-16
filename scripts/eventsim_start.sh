#!/usr/bin/env bash
# Broker healthchecks call the Kafka API; never replace an existing Eventsim.
set -euo pipefail
EVENTSIM_IMAGE="${EVENTSIM_IMAGE:-eventsim:2.0}"
EVENTSIM_CONTAINER="${EVENTSIM_CONTAINER:-eventsim-v2}"
KAFKA_BOOTSTRAP="${EVENTSIM_KAFKA_BOOTSTRAP_SERVERS:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"
KAFKA_NETWORK="${EVENTSIM_KAFKA_NETWORK:-eventsim-net}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_ROOT/data"
docker ps --format '{{.Names}}' >/dev/null
if [ -n "$(docker ps -a --filter "name=^${EVENTSIM_CONTAINER}$" --format '{{.Names}}')" ]; then
    echo "Container '$EVENTSIM_CONTAINER' exists and was preserved. Choose a new EVENTSIM_CONTAINER or explicitly stop/start it." >&2
    exit 1
fi
deadline=$((SECONDS + 60))
while true; do
    ready=true
    for broker in kafka-1 kafka-2 kafka-3; do
        health=$(docker inspect "$broker" --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' 2>/dev/null || true)
        [ "$health" = healthy ] || ready=false
    done
    $ready && break
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo 'Kafka API healthchecks did not become healthy within 60s. Start infra/docker-compose.yml first.' >&2
        exit 1
    fi
    sleep 2
done
docker network inspect "$KAFKA_NETWORK" --format '{{.Name}}' >/dev/null
mkdir -p "$DATA_DIR"
docker run -d --name "$EVENTSIM_CONTAINER" --network "$KAFKA_NETWORK" \
    --volume "$DATA_DIR:/opt/eventsim/output_data" "$EVENTSIM_IMAGE" \
    -c examples/config.json --start-time 2026-09-01T00:00:00 \
    -n 100 --growth-rate 0 --userid 1 --randomseed 1 --nocontinuous \
    --kafkaBrokerList "$KAFKA_BOOTSTRAP"
echo "Eventsim started: $EVENTSIM_CONTAINER; bootstrap: $KAFKA_BOOTSTRAP"
echo "Inspect delivery/runtime errors with: docker logs -f $EVENTSIM_CONTAINER"
