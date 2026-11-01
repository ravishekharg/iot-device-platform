# syntax=docker/dockerfile:1
# Broker with authentication and per-device topic ACLs.
# Stage 1 provisions credentials: every simulated device id gets a password derived from the fleet secret
# (HMAC-SHA256(secret, device id)), the way a factory flashes a per-device secret; the platform services get their
# own users. Production replaces this with per-device X.509 client certificates (mTLS) on 8883.
FROM python:3.12-alpine AS credentials
ARG FLEET_SECRET=fleet-lab-only
ARG DEVICE_PREFIXES="sim-mqtt"
ARG DEVICE_CREDENTIALS=5000
ARG INGEST_PASSWORD=ingest-lab-only
ARG API_PASSWORD=api-lab-only
RUN python - <<'EOF'
import hashlib, hmac, os
secret = os.environ["FLEET_SECRET"].encode()
with open("/passwd", "w") as f:
    for prefix in os.environ["DEVICE_PREFIXES"].split():
        for i in range(int(os.environ["DEVICE_CREDENTIALS"])):
            device = f"{prefix}-{i:05d}"
            f.write(f"{device}:{hmac.new(secret, device.encode(), hashlib.sha256).hexdigest()[:32]}\n")
    f.write(f"ingest:{os.environ['INGEST_PASSWORD']}\napi:{os.environ['API_PASSWORD']}\n")
EOF

FROM eclipse-mosquitto:2.0.20
COPY --from=credentials /passwd /mosquitto/config/passwd
# hash every plain-text password in place (sha512-pbkdf2); the image never holds them in clear
RUN mosquitto_passwd -U /mosquitto/config/passwd && chown mosquitto:mosquitto /mosquitto/config/passwd \
 && chmod 0600 /mosquitto/config/passwd
COPY mosquitto.conf acl /mosquitto/config/
