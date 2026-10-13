# Protocols as implemented here

## LwM2M 1.1 over CoAP/UDP (`services/simulator/sim/lwm2m_device.py`)

| Step | On the wire | Here |
|---|---|---|
| Register | `POST coap://server/rd?ep=<id>&lt=<lifetime>&lwm2m=1.1&b=U` with `</>;rt="oma.lwm2m";ct=110,</1/0>,</3/0>,</4/0>,</3303/0>` (link format, CF 40) → `2.01 Created`, `Location-Path: rd/<reg-id>` | `LwM2MDevice.register()` |
| Update | `POST /rd/<reg-id>` before the lifetime runs out (60–80% of it, randomized so a fleet doesn't update in lockstep) | `LwM2MDevice.update()`; a failed update means the server forgot us → register again |
| Read | Server `GET /3/0` (instance) or `/3303/0/5700` (resource) | SenML JSON (CF 110); text when asked with Accept 0 |
| Write | Server `PUT /3/0/14` | Accepts TLV (CF 11542; what Leshan sent), SenML JSON and text |
| Execute | Server `POST /3/0/4` (Reboot) | Device de-registers, "boots" for 2–5 s, registers again |
| Observe / Notify | Server `GET` with Observe 0 on `/3303/0/5700`, `/3/0/9`, `/4/0/2`; device sends notifications | Started by `lwm2m-bridge` on every REGISTRATION event |
| De-register | `DELETE /rd/<reg-id>` | On clean shutdown |

Objects: `/1/0` Server (lifetime, binding), `/3/0` Device (manufacturer, model, serial, firmware,
reboot, battery, time, UTC offset), `/4/0` Connectivity Monitoring (RSSI), `/3303/0` IPSO
Temperature. Not modelled: DTLS (PSK/RPK/X.509), bootstrap, firmware update (`/5`), queue mode
sleeping (the binding `UQ` is supported in code but the simulator doesn't close its socket while
"asleep").

Leshan's side: REST API `GET /api/clients`, `GET|PUT|POST /api/clients/<ep>/<path>`,
`POST .../observe`, and a server-sent-events stream `GET /api/event` (needs
`Accept: text/event-stream`; events REGISTRATION, UPDATED, DEREGISTRATION, NOTIFICATION, SLEEPING,
AWAKE, COAPLOG).

## MQTT 5 (`services/simulator/sim/mqtt_device.py`)

| Feature | Use here |
|---|---|
| Persistent session (`clean_start=False`, session expiry 1 h) | Broker queues QoS 1 commands for offline devices: offline delivery without a custom queue (verified by `scripts/demo-commands.sh` step 3) |
| Last Will and Testament | `devices/<id>/status = offline`, retained, published by the broker on an unclean drop (step 4) |
| Retained status | New subscribers see each device's current state immediately |
| QoS 1 | Telemetry and commands at least once; consumers are idempotent (telemetry primary key) |
| Shared subscriptions (`$share/ingest/...`) | Broker load-balances telemetry across ingest replicas |
| Keep-alive 60 s | Below typical NAT/LB idle timeouts |

## Radio context (why the protocols look like this)
NB-IoT devices (deep coverage, ~tens of kbps, no handover) spend most of their life in PSM,
unreachable; they wake, send, listen briefly, sleep. That's why LwM2M runs over UDP (no handshake
to pay for on each wake), why registration lifetimes and updates exist, and why the server
queues requests (queue mode). LTE-M trackers (~1 Mbps, mobility, lower latency) can keep an
MQTT/TCP connection open.
