"""Northbound API: what an enterprise application integrates with.

GET  /devices                    registry, filterable by status and protocol
GET  /devices/{id}               one device
GET  /devices/{id}/telemetry     latest readings
GET  /stats                      fleet overview (online per protocol, readings in the last minute)
POST /devices/{id}/commands      {"action": "reboot" | "set_interval" | "set_utc_offset", "value": ...}
GET  /devices/{id}/commands      command history with delivery and acknowledgement state

Commands go out on the device's own channel:
- MQTT: published QoS 1 to devices/<id>/commands. Devices keep persistent sessions, so the broker
  holds the command while a device is offline and delivers it when it reconnects: offline
  queueing without a custom queue. The device's ack comes back through ingest → Kafka → writer.
- LwM2M: executed or written through the LwM2M server's REST API (reboot = Execute /3/0/4,
  set_utc_offset = Write /3/0/14). Leshan's queue mode holds requests for sleeping devices.
"""
import json
import os
import uuid
from contextlib import asynccontextmanager

import aiomqtt
import asyncpg
import httpx
from fastapi import FastAPI, HTTPException
from prometheus_client import Counter, make_asgi_app
from pydantic import BaseModel

COMMANDS = Counter("api_commands_total", "Commands sent", ["channel", "action", "status"])
LESHAN = os.environ.get("LESHAN_API", "http://leshan:8080/api")
MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")

state: dict = {}


@asynccontextmanager
async def lifespan(_app: FastAPI):
    state["db"] = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=5)
    state["http"] = httpx.AsyncClient(timeout=15)
    yield
    await state["http"].aclose()
    await state["db"].close()


app = FastAPI(title="IoT device platform API", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


class Command(BaseModel):
    action: str
    value: str | float | None = None


def row(r) -> dict:
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(r).items()}


@app.get("/health")
async def health():
    async with state["db"].acquire() as c:
        await c.fetchval("SELECT 1")
    return {"status": "ok"}


@app.get("/devices")
async def devices(status: str | None = None, protocol: str | None = None, limit: int = 100):
    q = "SELECT * FROM devices WHERE ($1::text IS NULL OR status = $1) AND ($2::text IS NULL OR protocol = $2) ORDER BY device_id LIMIT $3"
    async with state["db"].acquire() as c:
        return [row(r) for r in await c.fetch(q, status, protocol, min(limit, 1000))]


@app.get("/devices/{device_id}")
async def device(device_id: str):
    async with state["db"].acquire() as c:
        r = await c.fetchrow("SELECT * FROM devices WHERE device_id = $1", device_id)
    if r is None:
        raise HTTPException(404, "unknown device")
    return row(r)


@app.get("/devices/{device_id}/telemetry")
async def telemetry(device_id: str, limit: int = 20):
    async with state["db"].acquire() as c:
        rows = await c.fetch("SELECT * FROM telemetry WHERE device_id = $1 ORDER BY ts DESC LIMIT $2", device_id, min(limit, 500))
    return [row(r) for r in rows]


@app.get("/stats")
async def stats():
    async with state["db"].acquire() as c:
        by = await c.fetch("SELECT protocol, status, count(*) AS devices FROM devices GROUP BY 1, 2 ORDER BY 1, 2")
        recent = await c.fetchval("SELECT count(*) FROM telemetry WHERE ts > now() - interval '1 minute'")
    return {"devices": [row(r) for r in by], "readings_last_minute": recent}


@app.post("/devices/{device_id}/commands", status_code=202)
async def send_command(device_id: str, cmd: Command):
    dev = await device(device_id)
    cmd_id = str(uuid.uuid4())
    if dev["protocol"] == "mqtt":
        if cmd.action not in ("reboot", "set_interval"):
            raise HTTPException(400, "MQTT devices support: reboot, set_interval")
        async with aiomqtt.Client(MQTT_HOST, identifier=f"api-{cmd_id[:8]}") as client:
            await client.publish(f"devices/{device_id}/commands",
                                 json.dumps({"id": cmd_id, "action": cmd.action, "value": cmd.value}), qos=1)
        delivery = "queued"   # the broker delivers it now, or when the device reconnects
    else:
        if cmd.action == "reboot":
            r = await state["http"].post(f"{LESHAN}/clients/{device_id}/3/0/4", params={"timeout": 10})
        elif cmd.action == "set_utc_offset":
            r = await state["http"].put(f"{LESHAN}/clients/{device_id}/3/0/14", params={"timeout": 10},
                                        json={"id": 14, "kind": "singleResource", "value": str(cmd.value), "type": "string"})
        else:
            raise HTTPException(400, "LwM2M devices support: reboot, set_utc_offset")
        ok = r.status_code == 200 and r.json().get("success")
        delivery = "delivered" if ok else "failed"
    async with state["db"].acquire() as c:
        await c.execute("INSERT INTO commands (id, device_id, action, value, channel, delivery_status) VALUES ($1, $2, $3, $4, $5, $6)",
                        uuid.UUID(cmd_id), device_id, cmd.action, None if cmd.value is None else str(cmd.value),
                        dev["protocol"], delivery)
    COMMANDS.labels(dev["protocol"], cmd.action, delivery).inc()
    return {"id": cmd_id, "channel": dev["protocol"], "delivery_status": delivery}


@app.get("/devices/{device_id}/commands")
async def command_history(device_id: str, limit: int = 20):
    async with state["db"].acquire() as c:
        rows = await c.fetch("SELECT * FROM commands WHERE device_id = $1 ORDER BY created_at DESC LIMIT $2", device_id, min(limit, 200))
    return [row(r) for r in rows]
