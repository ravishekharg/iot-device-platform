import json

from ingest import route


def test_telemetry_goes_to_telemetry_topic_keyed_by_device():
    topic, key, record = route("devices/d-1/telemetry", json.dumps({"temperature_c": 21.5, "ts": 1.0}).encode())
    assert (topic, key) == ("telemetry", "d-1")
    assert record == {"temperature_c": 21.5, "ts": 1.0, "device_id": "d-1", "protocol": "mqtt"}


def test_device_id_comes_from_the_topic_not_the_payload():
    # A device can't write telemetry for another device by lying in the payload.
    _, key, record = route("devices/d-1/telemetry", json.dumps({"device_id": "someone-else"}).encode())
    assert key == "d-1" and record["device_id"] == "d-1"


def test_status_becomes_a_device_event():
    topic, key, record = route("devices/d-2/status", b"offline")
    assert topic == "device-events" and key == "d-2"
    assert record["event"] == "offline" and record["protocol"] == "mqtt"


def test_unexpected_status_value_is_marked_unknown():
    _, _, record = route("devices/d-2/status", b"rebooting")
    assert record["event"] == "unknown"


def test_command_ack_routing():
    topic, key, record = route("devices/d-3/commands/ack", json.dumps({"id": "c1", "result": "ok"}).encode())
    assert (topic, key) == ("command-acks", "d-3")
    assert record["id"] == "c1" and record["result"] == "ok"


def test_unknown_topics_are_ignored():
    assert route("other/d-1/telemetry", b"{}") is None
    assert route("devices/d-1/firmware", b"{}") is None
