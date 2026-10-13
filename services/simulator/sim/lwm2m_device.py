"""A simulated LwM2M 1.1 client over CoAP/UDP.

What a real constrained device does, step by step:
1. Register with the LwM2M server: POST /rd?ep=<endpoint>&lt=<lifetime>&lwm2m=1.1&b=<binding>
   with the objects it supports in CoRE Link Format. The server replies 2.01 Created and a
   Location-Path (/rd/<registration-id>).
2. Send Registration Updates (POST /rd/<id>) before the lifetime runs out. This also refreshes
   NAT mappings in the operator network; a missed update means the server marks it offline.
3. Serve the server's requests on the same UDP socket: Read (GET), Observe (GET + Observe
   option, then notifications when values change), Write (PUT), Execute (POST).
4. De-register (DELETE /rd/<id>) on a clean shutdown.

Objects served (OMA registry): /1/0 Server, /3/0 Device (reboot is /3/0/4), /4/0 Connectivity
Monitoring, /3303/0 Temperature (IPSO). Payloads use SenML JSON (content format 110) for
object and instance reads and text/plain for single resources when asked.

Not modelled: DTLS (PSK/RPK/X.509), bootstrap and firmware update (/5). The server side of
those is Leshan's; see docs/protocols.md.
"""
import asyncio
import json
import logging
import random
import socket
import time
from urllib.parse import urlsplit

import aiocoap
from aiocoap import Code, Message, resource

from . import metrics
from .backoff import full_jitter_delay
from .device_model import SensorState

log = logging.getLogger("lwm2m")

CF_TEXT = 0
CF_LINK = 40
CF_SENML_JSON = 110
CF_TLV = 11542


def local_ip_towards(host: str, port: int) -> str:
    """The local IP this machine uses to reach host:port (no packet is sent for UDP connect)."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect((socket.gethostbyname(host), port))
        return s.getsockname()[0]


def decode_tlv_value(data: bytes) -> bytes:
    """Value of the first resource in an LwM2M TLV payload (servers often write in TLV).

    Header byte: bits 7-6 identifier type (11 = resource with value), bit 5 identifier length
    (0 = 8-bit, 1 = 16-bit), bits 4-3 length type (00 = length in bits 2-0, 01/10/11 = 8/16/24-bit
    length field follows the identifier), bits 2-0 the length itself when length type is 00.
    """
    header = data[0]
    pos = 1 + (2 if header & 0x20 else 1)          # skip the identifier
    length_type = (header >> 3) & 0x03
    if length_type == 0:
        length = header & 0x07
    else:
        length = int.from_bytes(data[pos:pos + length_type], "big")
        pos += length_type
    return data[pos:pos + length]


def _senml(base: str, values: dict[int, object]) -> bytes:
    """SenML JSON records for the given resource values under base name `base`."""
    records = []
    for i, (rid, value) in enumerate(values.items()):
        rec: dict = {"n": str(rid)}
        if i == 0:
            rec["bn"] = base
        if isinstance(value, bool):
            rec["vb"] = value
        elif isinstance(value, (int, float)):
            rec["v"] = value
        else:
            rec["vs"] = str(value)
        records.append(rec)
    return json.dumps(records).encode()


class LwM2MObjectInstance(resource.ObservableResource):
    """One object instance (/obj/inst). Single resources are served by ResourceValue below."""

    def __init__(self, path: str, getters: dict[int, callable], writers: dict[int, callable] | None = None):
        super().__init__()
        self.path = path              # e.g. "/3303/0"
        self.getters = getters        # resource id -> callable returning the current value
        self.writers = writers or {}

    def values(self) -> dict[int, object]:
        return {rid: get() for rid, get in self.getters.items()}

    async def render_get(self, request):
        if request.opt.observe is not None:
            metrics.MESSAGES.labels("lwm2m").inc()
        return Message(payload=_senml(self.path + "/", self.values()), content_format=CF_SENML_JSON)


class ResourceValue(resource.ObservableResource):
    """A single resource (/obj/inst/res): readable, optionally writable/executable, observable."""

    def __init__(self, path: str, getter=None, writer=None, executor=None):
        super().__init__()
        self.path, self.getter, self.writer, self.executor = path, getter, writer, executor

    async def render_get(self, request):
        if self.getter is None:
            return Message(code=Code.METHOD_NOT_ALLOWED)
        if request.opt.observe is not None:
            metrics.MESSAGES.labels("lwm2m").inc()
        value = self.getter()
        if request.opt.accept == CF_TEXT:
            return Message(payload=str(value).encode(), content_format=CF_TEXT)
        return Message(payload=_senml(self.path, {"": value}).replace(b'"n": "", ', b""), content_format=CF_SENML_JSON)

    async def render_put(self, request):
        if self.writer is None:
            return Message(code=Code.METHOD_NOT_ALLOWED)
        if request.opt.content_format == CF_TLV:
            payload = decode_tlv_value(request.payload).decode()
        else:
            payload = request.payload.decode()
        if request.opt.content_format == CF_SENML_JSON:
            rec = json.loads(payload)[0]
            payload = rec.get("vs", rec.get("v", rec.get("vb")))
        self.writer(payload)
        return Message(code=Code.CHANGED)

    async def render_post(self, request):
        if self.executor is None:
            return Message(code=Code.METHOD_NOT_ALLOWED)
        asyncio.get_running_loop().call_soon(self.executor)
        return Message(code=Code.CHANGED)


class LwM2MDevice:
    def __init__(self, endpoint: str, server_uri: str, lifetime: int = 120,
                 queue_mode: bool = False, report_interval: float = 30.0, jitter: bool = True):
        self.endpoint = endpoint
        self.server_uri = server_uri.rstrip("/")   # e.g. coap://leshan:5683
        self.lifetime = lifetime
        self.binding = "UQ" if queue_mode else "U"
        self.report_interval = report_interval
        self.jitter = jitter
        self.state = SensorState(device_id=endpoint)
        self.location: str | None = None
        self.ctx: aiocoap.Context | None = None
        self._rebooting = asyncio.Event()
        self.temperature = ResourceValue("/3303/0/5700", getter=lambda: self.state.temperature())
        self.battery = ResourceValue("/3/0/9", getter=lambda: int(self.state.battery_pct))
        self.rssi = ResourceValue("/4/0/2", getter=lambda: self.state.rssi_dbm)
        self.temp_instance = LwM2MObjectInstance("/3303/0", {
            5700: lambda: self.state.temperature(), 5701: lambda: "Cel",
            5601: lambda: round(self.state.base_temp - 3, 2), 5602: lambda: round(self.state.base_temp + 3, 2),
        })

    # ------------------------------------------------------------------ objects
    def _site(self) -> resource.Site:
        s, site = self.state, resource.Site()

        def add(path: str, res):
            site.add_resource(path.strip("/").split("/"), res)

        add("/1/0", LwM2MObjectInstance("/1/0", {0: lambda: 123, 1: lambda: self.lifetime, 7: lambda: self.binding}))
        add("/1/0/1", ResourceValue("/1/0/1", getter=lambda: self.lifetime))
        device_getters = {
            0: lambda: "OpenSim Devices", 1: lambda: "TH-100", 2: lambda: s.device_id, 3: lambda: s.firmware,
            9: lambda: int(s.battery_pct), 11: lambda: 0, 13: lambda: int(time.time()),
            14: lambda: s.utc_offset, 16: lambda: self.binding,
        }
        add("/3/0", LwM2MObjectInstance("/3/0", device_getters))
        for rid, get in device_getters.items():
            if rid not in (9, 14):
                add(f"/3/0/{rid}", ResourceValue(f"/3/0/{rid}", getter=get))
        add("/3/0/9", self.battery)
        add("/3/0/4", ResourceValue("/3/0/4", executor=self._reboot))
        add("/3/0/14", ResourceValue("/3/0/14", getter=lambda: s.utc_offset, writer=self._set_utc_offset))
        add("/4/0", LwM2MObjectInstance("/4/0", {2: lambda: s.rssi_dbm, 3: lambda: 10}))
        add("/4/0/2", self.rssi)
        add("/3303/0", self.temp_instance)
        add("/3303/0/5700", self.temperature)
        return site

    def _set_utc_offset(self, value) -> None:
        self.state.utc_offset = str(value)
        metrics.COMMANDS.labels("lwm2m", "write").inc()
        log.info("%s: UTC offset set to %s", self.endpoint, value)

    def _reboot(self) -> None:
        metrics.COMMANDS.labels("lwm2m", "reboot").inc()
        log.info("%s: reboot requested", self.endpoint)
        self._rebooting.set()

    # ------------------------------------------------------------------ registration
    @staticmethod
    def links() -> bytes:
        # ct=110: we prefer SenML JSON, so the server uses it for reads.
        return b'</>;rt="oma.lwm2m";ct=110,</1/0>,</3/0>,</4/0>,</3303/0>'

    async def register(self) -> bool:
        metrics.CONNECT_ATTEMPTS.labels("lwm2m").inc()
        uri = f"{self.server_uri}/rd?ep={self.endpoint}&lt={self.lifetime}&lwm2m=1.1&b={self.binding}"
        req = Message(code=Code.POST, uri=uri, payload=self.links(), content_format=CF_LINK)
        try:
            resp = await asyncio.wait_for(self.ctx.request(req).response, timeout=10)
        except Exception as e:  # network errors, timeouts
            metrics.CONNECT_FAILURES.labels("lwm2m").inc()
            log.warning("%s: register failed: %s", self.endpoint, e)
            return False
        if resp.code != Code.CREATED:
            metrics.CONNECT_FAILURES.labels("lwm2m").inc()
            log.warning("%s: register rejected: %s", self.endpoint, resp.code)
            return False
        self.location = "/" + "/".join(resp.opt.location_path)
        metrics.CONNECTED.labels("lwm2m").inc()
        log.info("%s: registered at %s", self.endpoint, self.location)
        return True

    async def update(self) -> bool:
        req = Message(code=Code.POST, uri=f"{self.server_uri}{self.location}")
        try:
            resp = await asyncio.wait_for(self.ctx.request(req).response, timeout=10)
            return resp.code == Code.CHANGED
        except Exception as e:
            log.warning("%s: update failed: %s", self.endpoint, e)
            return False

    async def deregister(self) -> None:
        if self.location:
            try:
                await asyncio.wait_for(self.ctx.request(Message(code=Code.DELETE, uri=f"{self.server_uri}{self.location}")).response, 5)
            except Exception:
                pass
            metrics.CONNECTED.labels("lwm2m").dec()
            self.location = None

    # ------------------------------------------------------------------ main loop
    async def run(self, stop: asyncio.Event) -> None:
        # One UDP socket per device on an ephemeral port, bound to the interface that reaches the
        # server (plain IPv4: containers usually have no IPv6). The server sends its requests
        # back to the address and port we registered from, exactly as with a real device.
        server = urlsplit(self.server_uri)
        local_ip = local_ip_towards(server.hostname, server.port or 5683)
        self.ctx = await aiocoap.Context.create_server_context(self._site(), bind=(local_ip, 0), transports=["simplesocketserver"])
        attempt = 0
        try:
            while not stop.is_set():
                if not self.location:
                    if not await self.register():
                        await asyncio.sleep(full_jitter_delay(attempt) if self.jitter else min(60, 2 ** attempt))
                        attempt += 1
                        continue
                    attempt = 0
                # Between updates: report sensor changes to observers, and handle reboots.
                next_update = time.monotonic() + self.lifetime * random.uniform(0.6, 0.8)
                while time.monotonic() < next_update and not stop.is_set() and not self._rebooting.is_set():
                    self.state.drain()
                    for observed in (self.temperature, self.temp_instance, self.battery, self.rssi):
                        observed.updated_state()
                    await asyncio.sleep(self.report_interval * random.uniform(0.8, 1.2))
                if self._rebooting.is_set():
                    self._rebooting.clear()
                    await self.deregister()
                    self.state.reboots += 1
                    await asyncio.sleep(random.uniform(2, 5))   # boot time
                    continue
                if not stop.is_set() and not await self.update():
                    # The server forgot us (restart, lifetime expired): register again.
                    metrics.CONNECTED.labels("lwm2m").dec()
                    self.location = None
        finally:
            await self.deregister()
            await self.ctx.shutdown()
