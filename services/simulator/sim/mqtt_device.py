"""A simulated MQTT device (e.g. an LTE-M tracker or a mains-powered gateway).

Behaviour that matters at fleet scale:
- Persistent session (MQTT 5 clean_start=False + session expiry): the broker keeps the device's
  subscriptions and queues QoS 1 commands while it's offline, and delivers them on reconnect.
- Last Will and Testament: if the connection drops without a clean disconnect, the broker
  publishes devices/<id>/status = "offline" (retained) on the device's behalf.
- Retained "online" status on connect, so any new subscriber sees current state immediately.
- QoS 1 telemetry (at least once): consumers must be idempotent.
- Reconnect with exponential backoff and full jitter (see backoff.py); --no-jitter shows the
  synchronized reconnect storm that jitter prevents.

Topics:
  devices/<id>/telemetry        device → platform   JSON reading, QoS 1
  devices/<id>/status           device → platform   "online" / "offline" (LWT), retained
  devices/<id>/commands         platform → device   {"id", "action", "value"}, QoS 1
  devices/<id>/commands/ack     device → platform   {"id", "result"}, QoS 1
"""
import asyncio
import json
import logging
import random

import aiomqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from . import metrics
from .backoff import exponential_delay, full_jitter_delay
from .device_model import SensorState

log = logging.getLogger("mqtt")


class MqttDevice:
    def __init__(self, device_id: str, host: str, port: int = 1883, report_interval: float = 10.0,
                 jitter: bool = True, session_expiry: int = 3600):
        self.device_id = device_id
        self.host, self.port = host, port
        self.report_interval = report_interval
        self.jitter = jitter
        self.session_expiry = session_expiry
        self.state = SensorState(device_id=device_id)
        self.base = f"devices/{device_id}"

    def _client(self) -> aiomqtt.Client:
        props = Properties(PacketTypes.CONNECT)
        props.SessionExpiryInterval = self.session_expiry
        return aiomqtt.Client(
            hostname=self.host, port=self.port, identifier=self.device_id,
            protocol=aiomqtt.ProtocolVersion.V5, clean_start=False, properties=props,
            keepalive=60,  # must stay below NAT and load-balancer idle timeouts
            will=aiomqtt.Will(topic=f"{self.base}/status", payload="offline", qos=1, retain=True),
        )

    async def _handle_command(self, client: aiomqtt.Client, payload: bytes) -> None:
        cmd = json.loads(payload)
        action = cmd.get("action", "unknown")
        metrics.COMMANDS.labels("mqtt", action).inc()
        result = "ok"
        if action == "set_interval":
            self.report_interval = float(cmd["value"])
        elif action == "reboot":
            self.state.reboots += 1
        else:
            result = "unsupported"
        log.info("%s: command %s -> %s", self.device_id, action, result)
        await client.publish(f"{self.base}/commands/ack", json.dumps({"id": cmd.get("id"), "result": result}), qos=1)

    async def _session(self) -> None:
        async with self._client() as client:
            metrics.CONNECTED.labels("mqtt").inc()
            try:
                await client.publish(f"{self.base}/status", "online", qos=1, retain=True)
                await client.subscribe(f"{self.base}/commands", qos=1)

                async def commands():
                    async for message in client.messages:
                        await self._handle_command(client, message.payload)

                async def telemetry():
                    # Random first delay so a fleet started together doesn't report in lockstep.
                    await asyncio.sleep(random.uniform(0, self.report_interval))
                    while True:
                        await client.publish(f"{self.base}/telemetry", json.dumps(self.state.reading()), qos=1)
                        metrics.MESSAGES.labels("mqtt").inc()
                        await asyncio.sleep(self.report_interval * random.uniform(0.9, 1.1))

                try:
                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(commands())
                        tg.create_task(telemetry())
                except asyncio.CancelledError:
                    # Deliberate shutdown (or going to sleep): say so before a clean disconnect.
                    # The last will only fires on an unclean drop, so it can't cover this case.
                    try:
                        await asyncio.wait_for(
                            client.publish(f"{self.base}/status", "offline", qos=1, retain=True), timeout=3)
                    except Exception:
                        pass
                    raise
            finally:
                metrics.CONNECTED.labels("mqtt").dec()

    async def run(self, stop: asyncio.Event) -> None:
        attempt = 0
        while not stop.is_set():
            metrics.CONNECT_ATTEMPTS.labels("mqtt").inc()
            try:
                session = asyncio.create_task(self._session())
                stopper = asyncio.create_task(stop.wait())
                done, _ = await asyncio.wait({session, stopper}, return_when=asyncio.FIRST_COMPLETED)
                if stopper in done:
                    session.cancel()
                    await asyncio.gather(session, return_exceptions=True)
                    return
                stopper.cancel()
                session.result()   # re-raise the connection error, if any
            except (aiomqtt.MqttError, OSError, ExceptionGroup) as e:
                metrics.CONNECT_FAILURES.labels("mqtt").inc()
                delay = full_jitter_delay(attempt) if self.jitter else exponential_delay(attempt)
                log.debug("%s: disconnected (%s), retrying in %.1fs", self.device_id, e, delay)
                attempt = min(attempt + 1, 10)
                await asyncio.sleep(delay)
                continue
            attempt = 0
