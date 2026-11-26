# Dependencies

What xenny needs: the tools on your machine, the platform components ArgoCD installs, and the
libraries the apps are built with. "Tested with" is what the dev cluster on the MacBook runs;
newer versions are likely fine.

## Tools on your machine

| Tool | Needed for | Tested with |
|---|---|---|
| Docker (Docker Desktop on macOS) | Runs the k3d dev cluster; local image builds | 29.8 |
| [k3d](https://k3d.io) | Creates the dev cluster from `bootstrap/k3d-dev.yaml` | 5.9.0 |
| kubectl | Everything that talks to the cluster | 1.37 |
| Helm | `bootstrap/install.sh` installs ArgoCD with it | 4.3.1 |
| bash, curl, openssl, base64 | `bootstrap/install.sh` and `tools/*.sh` (macOS's bash 3.2 is enough) | macOS built-ins |
| AWS CLI v2 | Uploading files to the landing bucket | 2.12 |
| [DuckDB](https://duckdb.org) CLI | Querying the lake, `sql/explore.sql`, `tools/reconcile.sh`; downloads its `iceberg` and `httpfs` extensions on first use | 2.0.0 |
| Java 25 (Temurin) and Maven | Building and testing the apps, and before building an image locally (CI builds the published images) | 25.0.4, Maven 3.10 |
| git | Changes reach the cluster through GitHub | 2.37 |

Optional: the [ntfy](https://ntfy.sh) app to receive alerts, and a SQL client such as DataGrip
(with DuckDB JDBC driver `org.duckdb:duckdb_jdbc` 2.0.0.1) for the lake.

**Machine:** give Docker at least 16 GB of memory; the cluster's pods request about 12.5 GB.
Volumes are thin-provisioned, so disk use grows with the data (a few GB per month of trips across
Kafka, the lake and ClickHouse).

## Network access

The cluster and tools reach these at install time or while running:

| Service | Used for |
|---|---|
| GitHub (`github.com/bryaneanders/xenny`) | ArgoCD syncs the cluster from `main`; Actions runs CI |
| GitHub Container Registry (`ghcr.io/bryaneanders`) | The `xenny-ingest`, `xenny-cleaner` and `xenny-connect` images, built by CI |
| Docker Hub, quay.io | Platform images (Kafka, ClickHouse, Grafana, ...) |
| Helm chart repositories | Listed in `bootstrap/apps/*.yaml` and `bootstrap/install.sh` |
| TLC CloudFront (`d37ci6vzurychx.cloudfront.net`) | Trip files, and the zone lookup CSV the cleaner downloads each time it starts |
| grafana.com | Grafana downloads its ClickHouse plugin when it starts |
| ntfy.sh | Alert notifications |

## Platform components

ArgoCD installs these from git; the version is pinned in the file listed.

| Component | Version | Pinned in |
|---|---|---|
| k3s (dev cluster) | v1.36.5 | `bootstrap/k3d-dev.yaml` |
| ArgoCD | chart 10.11.1 | `bootstrap/install.sh` |
| kube-prometheus-stack (Prometheus, Alertmanager, Grafana 13.2) | chart 92.13.1 | `bootstrap/apps/kube-prometheus-stack.yaml` |
| Grafana ClickHouse plugin | 4.23.4 | `bootstrap/apps/kube-prometheus-stack.yaml` |
| Strimzi operator | 1.3.0 | `bootstrap/apps/strimzi.yaml` |
| Kafka (KRaft, 3 brokers) | 4.3.1 | `platform/kafka/kafka.yaml` |
| Apicurio Registry | 3.3.4 | `platform/apicurio/registry.yaml` |
| SeaweedFS (S3 object store) | chart 4.48.0 | `bootstrap/apps/seaweedfs.yaml` |
| Lakekeeper (Iceberg REST catalog, with Postgres 18) | chart 0.12.0, image v0.13.8 | `bootstrap/apps/lakekeeper.yaml` |
| Kafka Connect image | Strimzi `kafka:1.3.0-kafka-4.3.1` base | `apps/connect/Dockerfile` |
| Altinity ClickHouse operator | chart 0.27.4 | `bootstrap/apps/clickhouse.yaml` |
| ClickHouse | 26.8.36.3 (LTS) | `platform/clickhouse/clickhouse.yaml` |

Jobs that run after a sync use `python:3.13-alpine` (schema registration), `curlimages/curl:8.23.0`
(Lakekeeper warehouse) and the ClickHouse image (DDL).

## Java libraries

Versions are set in the root `pom.xml`, except Iceberg (`apps/connect/pom.xml`).

| Library | Version | Used by |
|---|---|---|
| Kafka clients, Kafka Streams | 4.3.1 (matches the brokers) | ingest, cleaner |
| Avro | 1.12.2 | `schemas` (generated classes), all apps |
| Apicurio Registry serde | 3.3.4 | ingest, cleaner |
| Parquet, Hadoop client | 1.18.2, 3.5.0 | ingest (reading trip files) |
| AWS SDK | 2.59.1 | ingest (S3) |
| Micrometer (Prometheus registry) | 1.17.2 | cleaner metrics |
| SLF4J | 2.0.20 | logging |
| JUnit | 6.1.3 | tests |
| Iceberg Kafka Connect sink | 1.12.0 | Connect image |
| Apicurio Avro converter | 3.3.4 | Connect image |

Jackson, lz4-java, Netty and snappy-java are pinned in the root `pom.xml` above the versions other
libraries pull in, to pick up security fixes; CI fails an image with a CRITICAL vulnerability.
