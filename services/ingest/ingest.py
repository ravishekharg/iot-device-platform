"""MQTT → Kafka ingestion.

Uses an MQTT 5 *shared subscription* ($share/ingest/...): run several replicas and the broker
load-balances messages between them, so ingestion scales horizontally and survives a replica
restart. Every message is produced to Kafka keyed by device id (one partition per device keeps
each device's readings in order) with acks=all and idempotence on.

Throughput: messages are handed to the producer without waiting for each acknowledgement (the
producer batches them), and acknowledgements are confirmed in groups. Awaiting every send in turn
capped each replica at ~50 msg/s (one 20 ms linger per message): found by scripts/load-test.sh.

Delivery: QoS 1 from devices, at-least-once into Kafka. The MQTT library acknowledges on receipt,
so a crash between receive and produce can lose a message; a production version would use manual
acknowledgement (ack after Kafka confirms). Downstream consumers are idempotent either way.
"""
import asyncio
import json
import logging
import os
import socket
import time

import aiomqtt
from aiokafka import AIOKafkaProducer
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from prometheus_client import Counter, Histogram, start_http_server

log = logging.getLogger("ingest")
MESSAGES = Counter("ingest_messages_total", "MQTT messages ingested", ["kind"])
ERRORS = Counter("ingest_errors_total", "Messages that could not be ingested", ["reason"])
PRODUCE_LATENCY = Histogram("ingest_produce_seconds", "Time to confirm a batch of Kafka acks", buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1))

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
GROUP = os.environ.get("SHARE_GROUP", "ingest")


def route(topic: str, payload: bytes) -> tuple[str, str, dict] | None:
    """Map an MQTT message to (kafka topic, key, record). Returns None for unknown topics."""
    parts = topic.split("/")
    if len(parts) < 3 or parts[0] != "devices":
        return None
    device_id, kind = parts[1], "/".join(parts[2:])
    now = round(time.time(), 3)
    if kind == "telemetry":
        record = json.loads(payload)
        record.update(device_id=device_id, protocol="mqtt")
        return "telemetry", device_id, record
    if kind == "status":
        state = payload.decode()
        return "device-events", device_id, {"device_id": device_id, "protocol": "mqtt",
                                             "event": state if state in ("online", "offline") else "unknown", "ts": now}
    if kind == "commands/ack":
        record = json.loads(payload)
        record.update(device_id=device_id, ts=now)
        return "command-acks", device_id, record
    return None


async def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(name)s %(levelname)s %(message)s")
    start_http_server(9100)
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA, acks="all", enable_idempotence=True,
                                linger_ms=20, compression_type="gzip",
                                value_serializer=lambda v: json.dumps(v).encode(), key_serializer=str.encode)
    while True:
        try:
            await producer.start()
            break
        except Exception as e:
            log.warning("Kafka not ready (%s); retrying", e)
            await asyncio.sleep(3)

    props = Properties(PacketTypes.CONNECT)
    props.SessionExpiryInterval = 3600   # keep our queued messages across a restart
    client_id = f"{GROUP}-{socket.gethostname()}"
    while True:
        try:
            async with aiomqtt.Client(MQTT_HOST, identifier=client_id, protocol=aiomqtt.ProtocolVersion.V5,
                                      username=os.environ.get("MQTT_USERNAME", "ingest"),
                                      password=os.environ.get("MQTT_PASSWORD", "ingest-lab-only"),
                                      clean_start=False, properties=props) as client:
                for t in ("devices/+/telemetry", "devices/+/status", "devices/+/commands/ack"):
                    await client.subscribe(f"$share/{GROUP}/{t}", qos=1)
                log.info("subscribed as %s in share group %s", client_id, GROUP)
                pending: list[asyncio.Future] = []
                last_flush = time.monotonic()

                async def flush():
                    nonlocal pending, last_flush
                    if pending:
                        batch, pending = pending, []
                        with PRODUCE_LATENCY.time():
                            results = await asyncio.gather(*batch, return_exceptions=True)
                        errors = sum(isinstance(r, Exception) for r in results)
                        if errors:
                            ERRORS.labels("kafka").inc(errors)
                    last_flush = time.monotonic()

                async for message in client.messages:
                    try:
                        routed = route(str(message.topic), message.payload)
                    except (ValueError, UnicodeDecodeError):
                        ERRORS.labels("bad_payload").inc()
                        continue
                    if routed is None:
                        ERRORS.labels("unknown_topic").inc()
                        continue
                    topic, key, record = routed
                    pending.append(await producer.send(topic, record, key=key))   # queued, not yet acknowledged
                    MESSAGES.labels(topic).inc()
                    if len(pending) >= 500 or time.monotonic() - last_flush > 0.25:
                        await flush()
        except aiomqtt.MqttError as e:
            log.warning("MQTT connection lost (%s); reconnecting", e)
            await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())
