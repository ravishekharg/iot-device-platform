"""Kafka → PostgreSQL writer.

At-least-once with idempotent writes: messages are inserted in batches and the Kafka offsets are
committed only after the database transaction commits. A crash replays the uncommitted batch,
and the primary keys make replays harmless (ON CONFLICT DO NOTHING / upserts).

- telemetry      → telemetry table (one row per device per timestamp)
- device-events  → devices table (status, last_seen, firmware, protocol)
- command-acks   → commands table (acknowledged_at, result)
Consumer lag (messages not yet written) is exported per partition: it's the first metric to
watch when ingestion outpaces storage.
"""
import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import asyncpg
from aiokafka import AIOKafkaConsumer
from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("writer")
ROWS = Counter("writer_rows_total", "Rows written", ["table"])
LAG = Gauge("writer_consumer_lag", "Messages not yet consumed", ["topic", "partition"])
BATCH = int(os.environ.get("BATCH_SIZE", "500"))

STATUS = {"registered": "online", "online": "online", "seen": "online", "awake": "online",
          "sleeping": "sleeping", "offline": "offline"}


def ts(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


async def write_batch(conn: asyncpg.Connection, batch: list) -> None:
    telemetry, events, acks = [], [], []
    for msg in batch:
        rec = msg.value
        if msg.topic == "telemetry":
            telemetry.append((rec["device_id"], ts(rec["ts"]), rec.get("temperature_c"), rec.get("battery_pct"),
                              rec.get("rssi_dbm"), rec.get("protocol")))
        elif msg.topic == "device-events":
            events.append(rec)
        elif msg.topic == "command-acks":
            acks.append((rec.get("id"), rec.get("result"), ts(rec["ts"])))
    async with conn.transaction():
        if telemetry:
            await conn.executemany(
                """INSERT INTO telemetry (device_id, ts, temperature_c, battery_pct, rssi_dbm, protocol)
                   VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING""", telemetry)
            await conn.executemany(
                """INSERT INTO devices (device_id, protocol, status, last_seen) VALUES ($1, $2, 'online', $3)
                   ON CONFLICT (device_id) DO UPDATE SET last_seen = GREATEST(devices.last_seen, EXCLUDED.last_seen)""",
                [(t[0], t[5], t[1]) for t in telemetry])
            ROWS.labels("telemetry").inc(len(telemetry))
        for e in events:
            await conn.execute(
                """INSERT INTO devices (device_id, protocol, status, last_seen, firmware, model, registered_at)
                   VALUES ($1, $2, $3, $4::timestamptz, $5, $6, CASE WHEN $7::boolean THEN $4::timestamptz END)
                   ON CONFLICT (device_id) DO UPDATE SET
                     status = EXCLUDED.status,
                     last_seen = GREATEST(devices.last_seen, EXCLUDED.last_seen),
                     firmware = COALESCE(EXCLUDED.firmware, devices.firmware),
                     model = COALESCE(EXCLUDED.model, devices.model),
                     registered_at = COALESCE(EXCLUDED.registered_at, devices.registered_at)""",
                e["device_id"], e["protocol"], STATUS.get(e["event"], "unknown"), ts(e["ts"]),
                e.get("firmware"), e.get("model"), e["event"] == "registered")
        if events:
            ROWS.labels("devices").inc(len(events))
        if acks:
            await conn.executemany(
                "UPDATE commands SET result = $2, acknowledged_at = $3 WHERE id::text = $1 AND acknowledged_at IS NULL", acks)
            ROWS.labels("commands").inc(len(acks))


async def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(name)s %(levelname)s %(message)s")
    start_http_server(9100)
    consumer = AIOKafkaConsumer("telemetry", "device-events", "command-acks",
                                bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
                                group_id="writer", enable_auto_commit=False, auto_offset_reset="earliest",
                                value_deserializer=lambda v: json.loads(v))
    while True:
        try:
            await consumer.start()
            break
        except Exception as e:
            log.warning("Kafka not ready (%s); retrying", e)
            await asyncio.sleep(3)
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=4)
    try:
        while True:
            result = await consumer.getmany(timeout_ms=1000, max_records=BATCH)
            batch = [m for msgs in result.values() for m in msgs]
            if batch:
                async with pool.acquire() as conn:
                    await write_batch(conn, batch)
                await consumer.commit()   # only after the rows are safely in the database
            for tp in consumer.assignment():
                end = consumer.highwater(tp)
                if end is not None:
                    LAG.labels(tp.topic, tp.partition).set(max(0, end - await consumer.position(tp)))
    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
