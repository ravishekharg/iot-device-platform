# Eclipse Leshan demo server: an LwM2M server with a web UI and a REST API.
# The jar is downloaded at build time and verified by SHA-256 (pinned below).
FROM eclipse-temurin:21-jre
ARG LESHAN_VERSION=2.0.0-M18
ARG LESHAN_SHA256=734b5b8487aa208847b676e7f543744998c6b6ab7bc154551220336d633b02da
WORKDIR /opt/leshan
ADD --checksum=sha256:${LESHAN_SHA256}     https://repo1.maven.org/maven2/org/eclipse/leshan/leshan-demo-server/${LESHAN_VERSION}/leshan-demo-server-${LESHAN_VERSION}-jar-with-dependencies.jar     leshan-server.jar
RUN useradd --uid 10001 --no-create-home leshan && chown -R leshan /opt/leshan
USER 10001
# 5683/udp CoAP, 5684/udp CoAP over DTLS, 8080/tcp web UI + REST API
EXPOSE 5683/udp 5684/udp 8080/tcp
ENTRYPOINT ["java", "-Xms128m", "-Xmx384m", "-jar", "leshan-server.jar", "--web-port", "8080"]
