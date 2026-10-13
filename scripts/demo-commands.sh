#!/usr/bin/env bash
# End-to-end command test against the running stack (docker compose up first).
#  1. MQTT command to an online device → delivered, acknowledged by the device, ack stored.
#  2. LwM2M reboot through the LwM2M server → device reboots and registers again.
#  3. Offline queueing: stop the fleet, send an MQTT command, start the fleet → the broker
#     delivers the queued command from the device's persistent session and the ack arrives.
#  4. Crash: kill the fleet without a clean disconnect → the broker publishes each device's
#     last will ("offline").
set -euo pipefail
cd "$(dirname "$0")/.."
API=${API:-http://localhost:8096}
fail=0
json() { python3 -c "import sys,json; d=json.load(sys.stdin); print($1)"; }

echo "== 1. MQTT command to an online device"
ID=$(curl -s -X POST "$API/devices/sim-mqtt-00001/commands" -H 'content-type: application/json' \
  -d '{"action":"set_interval","value":15}' | json 'd["id"]')
sleep 8
ACK=$(curl -s "$API/devices/sim-mqtt-00001/commands?limit=5" | json "[c for c in d if c['id']=='$ID'][0]['result']")
echo "   command $ID → device result: $ACK"
[ "$ACK" = "ok" ] || fail=1

echo "== 2. LwM2M reboot via Leshan (Execute /3/0/4)"
BEFORE=$(curl -s "$API/devices/sim-lwm2m-00001" | json 'd["registered_at"]')
STATUS=$(curl -s -X POST "$API/devices/sim-lwm2m-00001/commands" -H 'content-type: application/json' \
  -d '{"action":"reboot"}' | json 'd["delivery_status"]')
sleep 15
AFTER=$(curl -s "$API/devices/sim-lwm2m-00001" | json 'd["registered_at"]')
echo "   delivery: $STATUS · registered_at before $BEFORE → after $AFTER"
[ "$STATUS" = "delivered" ] && [ "$BEFORE" != "$AFTER" ] || fail=1

echo "== 3. Offline queueing for MQTT devices"
docker compose stop simulator >/dev/null
sleep 5
OFFLINE=$(curl -s "$API/devices/sim-mqtt-00002" | json 'd["status"]')
echo "   fleet stopped; sim-mqtt-00002 status: $OFFLINE (published by the device on a clean shutdown)"
ID=$(curl -s -X POST "$API/devices/sim-mqtt-00002/commands" -H 'content-type: application/json' \
  -d '{"action":"reboot"}' | json 'd["id"]')
sleep 3
PENDING=$(curl -s "$API/devices/sim-mqtt-00002/commands?limit=5" | json "[c for c in d if c['id']=='$ID'][0]['acknowledged_at']")
echo "   command sent while offline; acknowledged_at: $PENDING"
docker compose start simulator >/dev/null
for _ in $(seq 1 30); do
  ACK=$(curl -s "$API/devices/sim-mqtt-00002/commands?limit=5" | json "[c for c in d if c['id']=='$ID'][0]['result']")
  [ "$ACK" = "ok" ] && break
  sleep 3
done
echo "   fleet restarted; device result: $ACK"
[ "$OFFLINE" = "offline" ] && [ "$PENDING" = "None" ] && [ "$ACK" = "ok" ] || fail=1

echo "== 4. Crash: the broker publishes the device's last will"
sleep 20
docker compose kill simulator >/dev/null          # SIGKILL: no clean MQTT disconnect
sleep 5
CRASHED=$(curl -s "$API/devices/sim-mqtt-00003" | json 'd["status"]')
echo "   fleet killed; sim-mqtt-00003 status: $CRASHED (from its MQTT last-will message)"
docker compose start simulator >/dev/null
[ "$CRASHED" = "offline" ] || fail=1

[ $fail -eq 0 ] && echo "ALL PASSED" || echo "FAILED"
exit $fail
