#!/usr/bin/env bash
# Measures what a broker outage does to a fleet, with and without reconnect jitter.
#
# For each mode: run N MQTT devices, wait until all are connected, take the broker down for
# OUTAGE seconds, bring it back, and sample the simulator's counters every second throughout.
# Reports the peak connection attempts in one second, how bursty the retries were, and how long
# after the broker came back every device was connected again.
#
#   scripts/reconnect-storm.sh                 # N=500, OUTAGE=20
#   N=1000 OUTAGE=30 scripts/reconnect-storm.sh
set -euo pipefail
cd "$(dirname "$0")/.."
N=${N:-500}
OUTAGE=${OUTAGE:-20}
AFTER=${AFTER:-60}
METRICS=${METRICS:-http://localhost:9101/metrics}

metric() {  # metric <name> <protocol>
  curl -s "$METRICS" | awk -v m="$1{protocol=\"$2\"}" '$1==m {print int($2)}'
}

run() {
  local jitter=$1
  echo "== JITTER=$jitter, $N MQTT devices, broker down for ${OUTAGE}s"
  MQTT_DEVICES=$N LWM2M_DEVICES=0 JITTER=$jitter RAMP_PER_SECOND=200 \
    docker compose up -d --force-recreate simulator >/dev/null 2>&1
  for _ in $(seq 1 120); do
    [ "$(metric sim_devices_connected mqtt)" = "$N" ] && break
    sleep 1
  done
  local start prev now conn delta peak=0 busy=0 recovered="" t
  start=$(metric sim_connect_attempts_total mqtt); prev=$start
  docker compose stop mosquitto >/dev/null 2>&1
  for t in $(seq 1 $((OUTAGE + AFTER))); do
    [ "$t" -eq $((OUTAGE + 1)) ] && docker compose start mosquitto >/dev/null 2>&1
    sleep 1
    now=$(metric sim_connect_attempts_total mqtt); conn=$(metric sim_devices_connected mqtt)
    delta=$((now - prev)); prev=$now
    [ "$delta" -gt "$peak" ] && peak=$delta
    [ "$delta" -ge $((N / 4)) ] && busy=$((busy + 1))   # seconds in which a quarter of the fleet retried at once
    if [ "$t" -gt "$OUTAGE" ] && [ -z "$recovered" ] && [ "$conn" = "$N" ]; then recovered=$((t - OUTAGE)); fi
  done
  echo "   connection attempts during the test:      $((now - start))"
  echo "   peak attempts in a single second:          $peak"
  echo "   seconds where ≥25% of the fleet retried:   $busy"
  echo "   seconds after the broker returned until all $N were connected: ${recovered:->$AFTER}"
}

run false
run true
echo "== Restoring the default simulator"
docker compose up -d --force-recreate simulator >/dev/null 2>&1
