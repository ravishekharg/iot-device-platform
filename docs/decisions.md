# Design decisions

### 1. Kafka between protocol gateways and storage
**Why:** decouples ingestion from processing: a slow database doesn't push back on devices, new
consumers (analytics, alerting, enterprise integrations) read the same stream independently, and
data can be replayed. Keyed by device id, so each device's readings stay in order.
**Trade-off:** one more system to run (MSK in production).

### 2. Offline commands through the protocols' own queueing
**Why:** MQTT persistent sessions (broker holds QoS 1 messages for offline clients) and LwM2M
queue mode (server holds requests for sleeping devices) already solve "deliver when the device is
back". A custom queue would duplicate them and drift from the device's real state.
**Trade-off:** queue limits live in the broker (`max_queued_messages`; beyond it the broker drops
messages) and must be monitored; command expiry needs MQTT 5 message expiry.

### 3. At-least-once everywhere, idempotent writes
**Why:** exactly-once across MQTT → Kafka → PostgreSQL isn't achievable end to end; at-least-once
plus idempotent storage (telemetry primary key `(device_id, ts)`, upserts for device state, offsets
committed after the DB transaction) gives "effectively once".

### 4. The LwM2M bridge decides what to observe
**Why:** observations belong to a registration and disappear when a device re-registers. The bridge
reacts to every REGISTRATION event, reads the Device object once and starts the observations, so
reboots and new devices are handled with no manual step.
**Trade-off:** a single consumer of Leshan's event stream (Recreate strategy on Kubernetes); at scale,
one bridge per Leshan replica.

### 5. Device identity from the channel, not the payload
`ingest` takes the device id from the MQTT topic (and in production, from the TLS client
certificate with topic ACLs), ignoring any id in the payload, so a device can't report as another.

### 6. Config baked into images, no bind mounts
**Why:** the same image runs in Compose, CI and Kubernetes; nothing depends on the host filesystem.
**Trade-off:** a config change means rebuilding the image (cheap, and versioned like code).

### 7. Simulator models behaviour, not just load
Persistent sessions, last will, jittered reconnects, registration lifetimes and updates, observe,
reboot via Execute: the failure modes that matter for fleets (reconnect storms, offline devices,
re-registration) can be reproduced and measured locally.
