"""Run a simulated device fleet.

Environment:
  MQTT_DEVICES       number of MQTT devices (default 50)
  LWM2M_DEVICES      number of LwM2M devices (default 10)
  MQTT_HOST/PORT     broker (default mosquitto:1883)
  LWM2M_SERVER       LwM2M server URI (default coap://leshan:5683)
  REPORT_INTERVAL    seconds between readings per device (default 10)
  LWM2M_LIFETIME     registration lifetime in seconds (default 120)
  JITTER             "false" disables reconnect jitter (to demonstrate a reconnect storm)
  RAMP_PER_SECOND    devices started per second (default 50): fleets connect gradually, not at once
  ID_PREFIX          device id prefix (default sim)
Metrics on :9100/metrics.
"""
import asyncio
import logging
import os
import signal

from prometheus_client import start_http_server

from .lwm2m_device import LwM2MDevice
from .mqtt_device import MqttDevice


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


async def main() -> None:
    logging.basicConfig(level=env("LOG_LEVEL", "INFO"), format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("coap-server").setLevel(logging.ERROR)   # aiocoap's per-request chatter
    start_http_server(9100)

    jitter = env("JITTER", "true").lower() != "false"
    interval = float(env("REPORT_INTERVAL", "10"))
    prefix = env("ID_PREFIX", "sim")
    devices = [
        MqttDevice(f"{prefix}-mqtt-{i:05d}", env("MQTT_HOST", "mosquitto"), int(env("MQTT_PORT", "1883")),
                   report_interval=interval, jitter=jitter)
        for i in range(int(env("MQTT_DEVICES", "50")))
    ] + [
        LwM2MDevice(f"{prefix}-lwm2m-{i:05d}", env("LWM2M_SERVER", "coap://leshan:5683"),
                    lifetime=int(env("LWM2M_LIFETIME", "120")), report_interval=interval, jitter=jitter)
        for i in range(int(env("LWM2M_DEVICES", "10")))
    ]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    ramp = float(env("RAMP_PER_SECOND", "50"))
    tasks = []
    for d in devices:
        tasks.append(asyncio.create_task(d.run(stop)))
        await asyncio.sleep(1 / ramp)
    logging.info("started %d devices", len(devices))
    await stop.wait()
    logging.info("stopping: deregistering devices")
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
