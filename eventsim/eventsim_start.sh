#!/usr/bin/env bash
# Broker healthchecks call the Kafka API; never replace an existing Eventsim.
set -euo pipefail
EVENTSIM_IMAGE="${EVENTSIM_IMAGE:-events:1.0}"
EVENTSIM_CONTAINER="${EVENTSIM_CONTAINER:-eventsim-v2}"
KAFKA_BOOTSTRAP="${EVENTSIM_KAFKA_BOOTSTRAP_SERVERS:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"
KAFKA_NETWORK="${EVENTSIM_KAFKA_NETWORK:-eventsim-net}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_ROOT/data"
BUILD_CONTEXT="$SCRIPT_DIR"
DOCKER_DATA_DIR="$DATA_DIR"
if command -v cygpath >/dev/null 2>&1; then
    # Docker Desktop expects Windows paths; preserve container paths in arguments.
    BUILD_CONTEXT="$(cygpath -m "$SCRIPT_DIR")"
    DOCKER_DATA_DIR="$(cygpath -m "$DATA_DIR")"
    export MSYS_NO_PATHCONV=1
fi
TODAY_UTC="$(date -u +%Y-%m-%d)"
START_TIME="${EVENTSIM_START_TIME:-${TODAY_UTC}T00:00:00}"
END_TIME="${EVENTSIM_END_TIME:-$(date -u -d "$TODAY_UTC +1 day" +%Y-%m-%dT00:00:00)}"
# GNU date is available on Linux and Git Bash. Override timestamps are UTC.
for timestamp in "$START_TIME" "$END_TIME"; do
    if [[ ! "$timestamp" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$ ]] ||
       [ "$(date -u -d "$timestamp UTC" +%Y-%m-%dT%H:%M:%S)" != "$timestamp" ]; then
        echo 'Invalid time: use UTC format YYYY-MM-DDTHH:MM:SS.' >&2
        exit 1
    fi
done
if [ "$(date -u -d "$END_TIME UTC" +%s)" -le "$(date -u -d "$START_TIME UTC" +%s)" ]; then
    echo 'EVENTSIM_END_TIME must be after EVENTSIM_START_TIME (UTC).' >&2
    exit 1
fi
for file in config/config.json target/eventsim-assembly-2.0.jar; do
    if [ ! -f "$SCRIPT_DIR/$file" ]; then
        echo "Missing Eventsim file: $file" >&2
        exit 1
    fi
done
docker ps --format '{{.Names}}' >/dev/null
existing=$(docker ps -a --filter "name=^${EVENTSIM_CONTAINER}$" --format '{{.Names}}')
if [ -n "$existing" ]; then
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
docker build --tag "$EVENTSIM_IMAGE" "$BUILD_CONTEXT"
docker run -d --name "$EVENTSIM_CONTAINER" --network "$KAFKA_NETWORK" \
    --env TZ=UTC --volume "$DOCKER_DATA_DIR:/opt/eventsim/output_data" "$EVENTSIM_IMAGE" \
    -c config/config.json --start-time "$START_TIME" --end-time "$END_TIME" \
    --nusers 20000 --nocontinuous --kafkaBrokerList "$KAFKA_BOOTSTRAP" \
    /opt/eventsim/output_data
sleep 1
runtime=$(docker inspect "$EVENTSIM_CONTAINER" --format '{{.State.Status}} {{.State.ExitCode}}')
case "$runtime" in
    'running 0'|'exited 0') ;;
    *) docker logs --tail 30 "$EVENTSIM_CONTAINER" >&2
       echo "Eventsim failed to start: $runtime" >&2
       exit 1 ;;
esac
echo "Eventsim: $EVENTSIM_CONTAINER; state: $runtime; bootstrap: $KAFKA_BOOTSTRAP"
echo "Simulation interval (UTC): $START_TIME -> $END_TIME; users: 20000"
echo "Inspect delivery/runtime errors with: docker logs -f $EVENTSIM_CONTAINER"
