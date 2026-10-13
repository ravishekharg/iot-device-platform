from bridge import notification_to_telemetry, parse_sse


def test_parse_sse_event():
    lines = ['event: NOTIFICATION',
             'data: {"ep":"d1","kind":"single","res":"/3303/0/5700","val":{"kind":"singleResource","id":5700,"type":"FLOAT","value":"20.57"}}']
    name, data = parse_sse(lines)
    assert name == "NOTIFICATION" and data["ep"] == "d1"


def test_parse_sse_ignores_incomplete_or_invalid_events():
    assert parse_sse(["event: COAPLOG"]) is None
    assert parse_sse(["data: {}"]) is None
    assert parse_sse(["event: X", "data: not json"]) is None


def test_notification_maps_observed_resources_to_telemetry_fields():
    t = notification_to_telemetry({"ep": "d1", "res": "/3303/0/5700", "val": {"value": "20.57"}})
    assert t["device_id"] == "d1" and t["temperature_c"] == 20.57 and t["protocol"] == "lwm2m"
    b = notification_to_telemetry({"ep": "d1", "res": "/3/0/9", "val": {"value": "87"}})
    assert b["battery_pct"] == 87.0


def test_notification_for_unobserved_resource_is_ignored():
    assert notification_to_telemetry({"ep": "d1", "res": "/3/0/13", "val": {"value": "1"}}) is None
