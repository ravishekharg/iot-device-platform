"""Generates dashboards/fleet.json. Dashboards are code: edit this file, not the JSON."""
import json
from pathlib import Path

PANELS = [
    ("Devices connected", "sum by (protocol) (sim_devices_connected)", "short", "{{protocol}}"),
    ("Telemetry sent by devices (msg/s)", "sum by (protocol) (rate(sim_messages_sent_total[1m]))", "short", "{{protocol}}"),
    ("Ingested into Kafka (msg/s)", "sum by (kind) (rate(ingest_messages_total[1m]))", "short", "{{kind}}"),
    ("Rows written to PostgreSQL (/s)", "sum by (table) (rate(writer_rows_total[1m]))", "short", "{{table}}"),
    ("Writer consumer lag", "sum by (topic) (writer_consumer_lag)", "short", "{{topic}}"),
    ("Connection attempts (/s): reconnect storms show here", "sum by (protocol) (rate(sim_connect_attempts_total[30s]))", "short", "{{protocol}}"),
    ("Kafka produce latency p95", "histogram_quantile(0.95, sum by (le) (rate(ingest_produce_seconds_bucket[5m])))", "s", "p95"),
    ("LwM2M server events (/s)", "sum by (event) (rate(bridge_events_total[1m]))", "short", "{{event}}"),
    ("Commands", "sum by (channel, action, status) (increase(api_commands_total[1h]))", "short", "{{channel}} {{action}} {{status}}"),
    ("Commands received by devices", "sum by (protocol, action) (increase(sim_commands_received_total[1h]))", "short", "{{protocol}} {{action}}"),
]


def panel(i: int, title: str, expr: str, unit: str, legend: str) -> dict:
    return {
        "id": i + 1, "type": "timeseries", "title": title,
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "gridPos": {"h": 8, "w": 12, "x": 12 * (i % 2), "y": 8 * (i // 2)},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        "targets": [{"refId": "A", "expr": expr, "legendFormat": legend}],
    }


dashboard = {
    "uid": "iot-fleet", "title": "IoT fleet", "schemaVersion": 39, "version": 1,
    "time": {"from": "now-30m", "to": "now"}, "refresh": "10s", "tags": ["iot"],
    "panels": [panel(i, *p) for i, p in enumerate(PANELS)],
}

out = Path(__file__).parent / "dashboards" / "fleet.json"
out.write_text(json.dumps(dashboard, indent=2) + "\n")
print(f"wrote {out}")
