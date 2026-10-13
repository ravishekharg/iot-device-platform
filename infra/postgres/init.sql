-- Device registry, telemetry and command log.

CREATE TABLE devices (
    device_id     text PRIMARY KEY,
    protocol      text NOT NULL,                 -- mqtt | lwm2m
    status        text NOT NULL DEFAULT 'unknown',  -- online | sleeping | offline
    last_seen     timestamptz,
    registered_at timestamptz,
    firmware      text,
    model         text
);
CREATE INDEX devices_status ON devices (status, protocol);

-- One row per device per reading. The primary key makes redelivered Kafka messages harmless.
-- At real scale this table would be partitioned by time (or live in a time-series store) with
-- retention; see docs/scaling.md.
CREATE TABLE telemetry (
    device_id     text NOT NULL,
    ts            timestamptz NOT NULL,
    temperature_c double precision,
    battery_pct   double precision,
    rssi_dbm      double precision,
    protocol      text,
    PRIMARY KEY (device_id, ts)
);
CREATE INDEX telemetry_ts ON telemetry (ts DESC);

CREATE TABLE commands (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id       text NOT NULL REFERENCES devices (device_id),
    action          text NOT NULL,
    value           text,
    channel         text NOT NULL,      -- mqtt (queued by the broker) | lwm2m (via Leshan)
    created_at      timestamptz NOT NULL DEFAULT now(),
    delivery_status text NOT NULL,      -- queued | delivered | failed
    result          text,
    acknowledged_at timestamptz
);
CREATE INDEX commands_device ON commands (device_id, created_at DESC);
