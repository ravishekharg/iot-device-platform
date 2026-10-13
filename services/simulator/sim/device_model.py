"""State of one simulated device: a battery-powered sensor reporting temperature."""
import math
import random
import time
from dataclasses import dataclass, field


@dataclass
class SensorState:
    device_id: str
    firmware: str = "1.4.2"
    battery_pct: float = field(default_factory=lambda: random.uniform(60, 100))
    rssi_dbm: int = field(default_factory=lambda: random.randint(-115, -70))
    base_temp: float = field(default_factory=lambda: random.uniform(18, 30))
    phase: float = field(default_factory=lambda: random.uniform(0, 2 * math.pi))
    reboots: int = 0
    utc_offset: str = "+05:30"

    def temperature(self, now: float | None = None) -> float:
        # A slow daily-ish wave plus noise, so dashboards show something realistic.
        t = time.time() if now is None else now
        return round(self.base_temp + 3 * math.sin(t / 600 + self.phase) + random.uniform(-0.3, 0.3), 2)

    def drain(self) -> None:
        self.battery_pct = max(0.0, self.battery_pct - random.uniform(0.001, 0.01))

    def reading(self) -> dict:
        self.drain()
        return {
            "device_id": self.device_id,
            "ts": round(time.time(), 3),
            "temperature_c": self.temperature(),
            "battery_pct": round(self.battery_pct, 2),
            "rssi_dbm": self.rssi_dbm,
            "firmware": self.firmware,
        }
