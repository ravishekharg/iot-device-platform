"""LwM2M server (Leshan) → Kafka bridge, and the platform's device-management logic for LwM2M.

Listens to Leshan's server-sent event stream:
- REGISTRATION: publish a "registered" device event, read the Device object once (firmware,
  model), and start Observing the resources the platform wants as telemetry. Observations
  are per registration, so they're re-created every time a device registers again.
- NOTIFICATION: turn observed values into telemetry records.
- UPDATED: registration refresh, recorded as a "seen" event (keeps last_seen current).
- DEREGISTRATION: publish "offline".
- SLEEPING / AWAKE: queue-mode devices (PSM) going to sleep and waking up.
"""
import asyncio
import json
import logging
import os
import time

import httpx
from aiokafka import AIOKafkaProducer
from prometheus_client import Counter, start_http_server

log = logging.getLogger("lwm2m-bridge")
EVENTS = Counter("bridge_events_total", "Leshan events processed", ["event"])
OBSERVE_FAILURES = Counter("bridge_observe_failures_total", "Observe requests that failed")

LESHAN = os.environ.get("LESHAN_API", "http://leshan:8080/api")
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

# Resources observed on every device, and the telemetry field each one feeds.
OBSERVED = {"/3303/0/5700": "temperature_c", "/3/0/9": "battery_pct", "/4/0/2": "rssi_dbm"}


def parse_sse(lines: list[str]) -> tuple[str, dict] | None:
    """One server-sent event (its lines) → (event name, JSON data)."""
    name, data = None, []
    for line in lines:
        if line.startswith("event:"):
            name = line[6:].strip()
        elif line.startswith("data:"):
            data.append(line[5:].strip())
    if not name or not data:
        return None
    try:
        return name, json.loads("".join(data))
    except json.JSONDecodeError:
        return None


def notification_to_telemetry(data: dict) -> dict | None:
    field = OBSERVED.get(data.get("res"))
    value = (data.get("val") or {}).get("value")
    if field is None or value is None:
        return None
    return {"device_id": data["ep"], "protocol": "lwm2m", "ts": round(time.time(), 3), field: float(value)}


class Bridge:
    def __init__(self, producer: AIOKafkaProducer, http: httpx.AsyncClient):
        self.producer, self.http = producer, http

    async def event(self, device_id: str, event: str, **attrs) -> None:
        await self.producer.send_and_wait("device-events", {"device_id": device_id, "protocol": "lwm2m",
                                                            "event": event, "ts": round(time.time(), 3), **attrs},
                                          key=device_id)

    async def on_registration(self, reg: dict) -> None:
        ep = reg["endpoint"]
        attrs = {"queue_mode": reg.get("queuemode", False), "lifetime": reg.get("lifetime")}
        try:
            r = await self.http.get(f"{LESHAN}/clients/{ep}/3/0", params={"timeout": 5})
            for res in r.json().get("content", {}).get("resources", []):
                if res["id"] == 3:
                    attrs["firmware"] = res["value"]
                elif res["id"] == 1:
                    attrs["model"] = res["value"]
        except Exception as e:
            log.warning("%s: could not read /3/0: %s", ep, e)
        await self.event(ep, "registered", **attrs)
        for path in OBSERVED:
            try:
                r = await self.http.post(f"{LESHAN}/clients/{ep}{path}/observe", params={"timeout": 5})
                if not r.json().get("success"):
                    raise RuntimeError(r.text[:120])
            except Exception as e:
                OBSERVE_FAILURES.inc()
                log.warning("%s: observe %s failed: %s", ep, path, e)

    async def handle(self, name: str, data: dict) -> None:
        EVENTS.labels(name).inc()
        if name == "REGISTRATION":
            asyncio.create_task(self.on_registration(data))
        elif name == "NOTIFICATION":
            record = notification_to_telemetry(data)
            if record:
                await self.producer.send_and_wait("telemetry", record, key=record["device_id"])
        elif name == "UPDATED":
            ep = (data.get("registration") or data).get("endpoint")
            if ep:
                await self.event(ep, "seen")
        elif name == "DEREGISTRATION":
            await self.event(data["endpoint"], "offline")
        elif name in ("SLEEPING", "AWAKE"):
            await self.event(data["ep"], name.lower())


async def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(name)s %(levelname)s %(message)s")
    start_http_server(9100)
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA, acks="all", enable_idempotence=True,
                                value_serializer=lambda v: json.dumps(v).encode(), key_serializer=str.encode)
    while True:
        try:
            await producer.start()
            break
        except Exception as e:
            log.warning("Kafka not ready (%s); retrying", e)
            await asyncio.sleep(3)

    async with httpx.AsyncClient(timeout=httpx.Timeout(10, read=None)) as http:
        bridge = Bridge(producer, http)
        while True:
            try:
                # Devices already registered before the bridge started: treat as new registrations.
                for reg in (await http.get(f"{LESHAN}/clients")).json():
                    asyncio.create_task(bridge.on_registration(reg))
                async with http.stream("GET", f"{LESHAN}/event", headers={"Accept": "text/event-stream"}) as stream:
                    log.info("connected to Leshan event stream")
                    lines: list[str] = []
                    async for line in stream.aiter_lines():
                        if line:
                            lines.append(line)
                            continue
                        parsed = parse_sse(lines)
                        lines = []
                        if parsed:
                            await bridge.handle(*parsed)
            except (httpx.HTTPError, OSError) as e:
                log.warning("Leshan event stream lost (%s); reconnecting", e)
                await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
