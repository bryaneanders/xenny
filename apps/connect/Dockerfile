# Kafka Connect with our plugins baked in. Build from the repo root:
#   docker build -f apps/connect/Dockerfile -t xenny-connect:dev .
FROM --platform=$BUILDPLATFORM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q package -pl apps/connect

# Keep the Kafka version in sync with platform/kafka/kafka.yaml.
FROM quay.io/strimzi/kafka:1.3.0-kafka-4.3.1
# Apply OS security fixes, and drop components only other Strimzi pods use.
USER root
RUN microdnf -y upgrade --nodocs && microdnf clean all && rm -rf /opt/cruise-control /opt/kafka-exporter
USER 1001
COPY --from=build /src/apps/connect/target/plugins/ /opt/kafka/plugins/
