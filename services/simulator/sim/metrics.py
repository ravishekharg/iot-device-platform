from prometheus_client import Counter, Gauge

CONNECTED = Gauge("sim_devices_connected", "Simulated devices currently connected/registered", ["protocol"])
MESSAGES = Counter("sim_messages_sent_total", "Telemetry messages sent", ["protocol"])
CONNECT_ATTEMPTS = Counter("sim_connect_attempts_total", "Connection/registration attempts", ["protocol"])
CONNECT_FAILURES = Counter("sim_connect_failures_total", "Failed connection/registration attempts", ["protocol"])
COMMANDS = Counter("sim_commands_received_total", "Commands received from the platform", ["protocol", "action"])
