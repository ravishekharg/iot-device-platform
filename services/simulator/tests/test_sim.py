import json
import random

from sim.backoff import exponential_delay, full_jitter_delay
from sim.device_model import SensorState
from sim.lwm2m_device import LwM2MDevice, _senml, decode_tlv_value


def test_exponential_delay_doubles_and_caps():
    assert [exponential_delay(a) for a in range(4)] == [1, 2, 4, 8]
    assert exponential_delay(20) == 60


def test_full_jitter_stays_within_bounds_and_spreads():
    rng = random.Random(42)
    delays = [full_jitter_delay(5, rng=rng) for _ in range(1000)]
    assert all(0 <= d <= 32 for d in delays)
    # Spread out, not clustered at the cap: that's the whole point.
    assert min(delays) < 3 and max(delays) > 29


def test_tlv_single_resource_short_and_8bit_length():
    assert decode_tlv_value(b"\xc6\x0e+01:00") == b"+01:00"          # length in header bits
    assert decode_tlv_value(b"\xc8\x0e\x0a0123456789") == b"0123456789"  # 8-bit length field
    assert decode_tlv_value(b"\xe8\x15\x7c\x03abc") == b"abc"         # 16-bit identifier (5500)


def test_senml_instance_payload():
    records = json.loads(_senml("/3303/0/", {5700: 21.5, 5701: "Cel"}))
    assert records == [{"n": "5700", "bn": "/3303/0/", "v": 21.5}, {"n": "5701", "vs": "Cel"}]


def test_registration_links_advertise_objects_and_senml():
    links = LwM2MDevice.links().decode()
    assert 'rt="oma.lwm2m";ct=110' in links
    for obj in ("</3/0>", "</3303/0>", "</1/0>"):
        assert obj in links


def test_sensor_reading_shape():
    r = SensorState(device_id="d1").reading()
    assert set(r) == {"device_id", "ts", "temperature_c", "battery_pct", "rssi_dbm", "firmware"}
    assert 0 <= r["battery_pct"] <= 100
