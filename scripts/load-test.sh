#!/usr/bin/env bash
# Runs a bigger fleet for a few minutes and reports throughput through each stage and the
# writer's lag, from Prometheus.
#
#   scripts/load-test.sh                       # 2000 MQTT + 50 LwM2M devices, every 5 s, 4 minutes
#   MQTT_DEVICES=5000 DURATION=300 scripts/load-test.sh
set -euo pipefail
cd "$(dirname "$0")/.."
MQTT_DEVICES=${MQTT_DEVICES:-2000}
LWM2M_DEVICES=${LWM2M_DEVICES:-50}
REPORT_INTERVAL=${REPORT_INTERVAL:-5}
DURATION=${DURATION:-240}
PROM=${PROM:-http://localhost:9095}

q() { curl -s --get "$PROM/api/v1/query" --data-urlencode "query=$1" | python3 -c '
import sys, json
r = json.load(sys.stdin)["data"]["result"]
print(" ".join(f"{float(x["value"][1]):.1f}" for x in r) or "n/a")'; }

echo "== $MQTT_DEVICES MQTT + $LWM2M_DEVICES LwM2M devices, one reading every ${REPORT_INTERVAL}s, for ${DURATION}s"
MQTT_DEVICES=$MQTT_DEVICES LWM2M_DEVICES=$LWM2M_DEVICES REPORT_INTERVAL=$REPORT_INTERVAL RAMP_PER_SECOND=200 \
  docker compose up -d --force-recreate simulator >/dev/null 2>&1
sleep "$DURATION"
echo "devices connected (mqtt, lwm2m):     $(q 'sum by (protocol) (sim_devices_connected)')"
echo "sent by devices, msg/s:              $(q 'sum(rate(sim_messages_sent_total[2m]))')"
echo "ingested into Kafka, msg/s:          $(q 'sum(rate(ingest_messages_total{kind="telemetry"}[2m]))')"
echo "telemetry rows written, rows/s:      $(q 'sum(rate(writer_rows_total{table="telemetry"}[2m]))')"
echo "writer lag now (messages):           $(q 'sum(writer_consumer_lag)')"
echo "writer lag max over the run:         $(q "max_over_time(sum(writer_consumer_lag)[${DURATION}s:15s])")"
echo "Kafka produce p95 (s):               $(q 'histogram_quantile(0.95, sum by (le) (rate(ingest_produce_seconds_bucket[2m])))')"
echo "ingest replicas sharing the load:    $(q 'sum by (instance) (rate(ingest_messages_total{kind="telemetry"}[2m]))')"
echo "== Restoring the default simulator"
docker compose up -d --force-recreate simulator >/dev/null 2>&1
