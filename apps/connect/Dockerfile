# Kafka Connect with our plugins baked in. Packages plugins Maven has already collected.
# From the repo root:
#   mvn -B package -pl apps/connect
#   docker build -f apps/connect/Dockerfile -t xenny-connect:dev .
# Keep the Kafka version in sync with platform/kafka/kafka.yaml.
FROM quay.io/strimzi/kafka:1.3.0-kafka-4.3.1
# Apply OS security fixes, and drop components only other Strimzi pods use.
USER root
RUN microdnf -y upgrade --nodocs && microdnf clean all && rm -rf /opt/cruise-control /opt/kafka-exporter
USER 1001
COPY apps/connect/target/plugins/ /opt/kafka/plugins/
