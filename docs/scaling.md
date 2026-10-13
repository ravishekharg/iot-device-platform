# From this stack to a million devices

The local stack runs a few thousand simulated devices on a laptop. This is what changes, and why,
on the way to a fleet of a million (the scale of telecom IoT platforms for utilities and logistics).

## Load estimate

| Assumption | Value |
|---|---|
| Fleet | 800k NB-IoT meters (LwM2M, wake every 15 min) + 200k LTE-M trackers (MQTT, every 30 s while moving, ~30% moving) |
| Meter traffic | 800,000 / 900 s ≈ **900 msg/s** |
| Tracker traffic | 60,000 moving / 30 s ≈ **2,000 msg/s** |
| Design peak | ×10 for reconnect storms and rush hour ≈ **20,000 msg/s** |
| Data | ~200 bytes/msg → ~50 GB/day raw, ~19 TB/year before compression |
| Connections | ~200k persistent MQTT connections; 800k LwM2M registrations (mostly asleep) |

The hard parts aren't raw message throughput (Kafka handles 20k msg/s on a small cluster). They
are **connection state** (hundreds of thousands of TCP and DTLS sessions), **reconnect storms**,
**sleepy devices behind carrier NAT**, and **multi-tenancy**.

## Component by component

| Here | At scale | Why |
|---|---|---|
| Mosquitto, single node | **EMQX / HiveMQ cluster** (or AWS IoT Core) behind an NLB, TLS 1.3 with per-device X.509 certificates, topic ACLs per device | One broker node is a single point of failure and tops out on connections; a cluster shares sessions and shared subscriptions across nodes |
| Leshan, one replica, in-memory registrations | Several Leshan replicas with **`--redis`** (ElastiCache) as the registration store, behind an **NLB with UDP listeners and source-IP stickiness**; **DTLS Connection ID** so sessions survive NAT rebinding | UDP needs a layer-4 LB (ALB can't do UDP); DTLS sessions are stateful; registrations must survive a replica restart |
| Kafka single broker, 6 partitions | **Amazon MSK**, 3 brokers across AZs, RF 3, `min.insync.replicas=2`, ~64 partitions per hot topic, tiered storage | Durability across AZs, consumer parallelism (writers scale up to the partition count), replay for new consumers |
| `ingest` ×2 | 10–50 replicas on the shared subscription, HPA on CPU or on broker queue depth (KEDA) | Shared subscriptions spread messages across replicas; each replica pipelines produces (load test: serial produce capped a replica at ~50 msg/s) |
| PostgreSQL `telemetry` table | **Time-partitioned** tables (TimescaleDB / native partitioning) or **Cassandra/Keyspaces** partitioned by `(tenant, device, month)`, 13 months hot, then Parquet on S3 queried with Athena | One big table with a B-tree index won't hold 19 TB/year; time partitioning makes retention a cheap partition drop |
| Device registry in PostgreSQL | Stays relational (Aurora PostgreSQL), sharded by tenant only if needed | Registry queries are relational: by tenant, group, firmware version |
| Commands via broker sessions / Leshan queue mode | Same, plus a command service with expiry, retries and per-tenant rate limits; **firmware campaigns** in cohorts (1% → 10% → 100%) gated on success rate | Sleepy devices only take commands when awake; a bad firmware rollout must stop itself |
| Prometheus + Grafana in compose | Prometheus per cluster with remote write to Amazon Managed Prometheus, per-tenant SLOs (ingestion success, command delivery within one wake cycle) | Long-term storage, global view, alerting that survives a cluster outage |

## Reconnect storms (measured here, mitigated there)

`scripts/reconnect-storm.sh` shows the mechanism on 500 devices: without jitter, retries arrive
in synchronized waves (9 seconds in which ≥25% of the fleet retried at once during a 20 s broker
outage, vs 3 with jitter). At a million devices those waves knock a recovering broker over
again. Mitigations, all needed together:
1. **Device firmware**: exponential backoff with full jitter and a minimum first-retry window of
   several seconds (here the first window is 0–1 s, which is why the single-second peak didn't drop).
2. **Broker**: connection-rate limits per listener and per source, TLS handshake offload, and
   capacity headroom for the reconnect burst.
3. **Platform**: Kafka absorbs the message burst that follows reconnection; ingest autoscaling.
4. **Operations**: alert on connection-attempt rate (`ReconnectStorm` rule) and on the fraction
   of the fleet connected (`FleetConnectivityDrop`).

## Multi-tenancy (an operator serving many enterprises)
Tenant is derived from the device's credentials (certificate subject or LwM2M endpoint
provisioning), never from the payload; Kafka topics or keys per tenant; row- or partition-level
isolation in storage; per-tenant API credentials (OAuth2 client credentials), quotas and usage
metering for billing.
