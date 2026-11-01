#!/usr/bin/env bash
# Device identity at the broker: ingest trusts the device id in the topic, so the broker must make sure only that
# device can publish there. Against the running stack (docker compose up), from inside the simulator container:
#   1. an anonymous client is refused
#   2. a device with a wrong password is refused
#   3. device A publishing a reading on device B's topic: dropped by the ACL, never reaches PostgreSQL
#   4. device A publishing on its own topic (positive control): reaches PostgreSQL
#   5. device A subscribed to device B's commands: B's commands are never delivered to A
# Exit 1 if any check fails.
set -euo pipefail
cd "$(dirname "$0")/.."
A=${A:-sim-mqtt-00001}
B=${B:-sim-mqtt-00002}
SPOOF=-77.25    # marker temperatures nobody's sensor model produces
OWN=-66.75

mqtt_ok=0
docker compose exec -T -e A="$A" -e B="$B" -e SPOOF="$SPOOF" -e OWN="$OWN" simulator python - <<'PY' || mqtt_ok=1
import asyncio, json, os, time
import aiomqtt
from sim.mqtt_device import device_secret

A, B = os.environ["A"], os.environ["B"]
results = []

def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS  " if ok else "FAIL  ") + name + (f"  ({detail})" if detail else ""))

async def refused(**kw):
    try:
        async with aiomqtt.Client("mosquitto", identifier=f"probe-{time.time_ns()}", **kw):
            return False, "connected"
    except aiomqtt.MqttError as e:
        return True, str(e)

async def main():
    ok, why = await refused()
    check("anonymous client refused", ok, why)
    ok, why = await refused(username=A, password="not-the-device-secret")
    check("wrong password refused", ok, why)
    reading = lambda t: json.dumps({"ts": round(time.time(), 3), "temperature_c": t, "battery_pct": 50.0, "rssi_dbm": -90.0})
    async with aiomqtt.Client("mosquitto", identifier=f"{A}-acl-test", username=A, password=device_secret(A),
                              protocol=aiomqtt.ProtocolVersion.V5) as c:
        await c.publish(f"devices/{B}/telemetry", reading(float(os.environ["SPOOF"])), qos=1)   # someone else's topic
        await asyncio.sleep(1.1)                                    # distinct ts for the second reading
        await c.publish(f"devices/{A}/telemetry", reading(float(os.environ["OWN"])), qos=1)      # its own topic
        # Mosquitto grants the subscription and enforces "read" when it delivers each message, so the test is
        # delivery: the API sends B a command, and A, subscribed to B's command topic, must receive nothing
        await c.subscribe(f"devices/{B}/commands", qos=1)
        async with aiomqtt.Client("mosquitto", identifier=f"api-acl-test-{time.time_ns()}", username="api",
                                  password="api-lab-only") as api:
            await api.publish(f"devices/{B}/commands", json.dumps({"id": "acl-test", "action": "noop"}), qos=1)
        leaked = []

        async def listen():
            async for m in c.messages:
                leaked.append(str(m.topic))
        try:
            await asyncio.wait_for(listen(), timeout=3)
        except asyncio.TimeoutError:
            pass
        check("another device's commands are never delivered to it", not leaked, leaked or "nothing received in 3 s")

asyncio.run(main())
raise SystemExit(0 if all(results) else 1)
PY

echo "   waiting 20 s for ingest -> Kafka -> writer -> PostgreSQL"
sleep 20
q() { docker compose exec -T postgres psql -U iot -d iot -At -c "$1"; }
spoofed=$(q "SELECT count(*) FROM telemetry WHERE device_id = '$B' AND temperature_c = $SPOOF")
own=$(q "SELECT count(*) FROM telemetry WHERE device_id = '$A' AND temperature_c = $OWN")
ok=$mqtt_ok
if [ "$spoofed" = 0 ]; then echo "PASS  reading published by $A on $B's topic never reached PostgreSQL"
else echo "FAIL  $spoofed spoofed reading(s) stored for $B"; ok=1; fi
if [ "$own" -ge 1 ]; then echo "PASS  $A's reading on its own topic reached PostgreSQL (positive control)"
else echo "FAIL  $A's own reading didn't arrive: the pipeline, not the ACL, may be broken"; ok=1; fi
[ "$ok" = 0 ]
