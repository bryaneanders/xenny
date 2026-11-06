# xenny
Xenny is a modularized and extensible streaming data ingestion framework running on Kubernetes.

## Layout

```
bootstrap/        # cluster config, install script, root app-of-apps
bootstrap/apps/   # one ArgoCD Application per component
platform/         # shared infra config (Kafka, schema registry, Iceberg catalog, Kafka Connect, ClickHouse)
schemas/          # Avro schemas, one per registry subject; registered by a Job on sync
pipelines/trips/  # topics, deployments, connectors, ClickHouse tables and dashboard for the trips dataset
apps/ingest/      # ingest service: files in landing/trips/ -> raw.trips
apps/cleaner/     # Kafka Streams app: raw.trips -> clean.trips (valid) and dlq.trips (rejected)
apps/connect/     # Kafka Connect image with the Iceberg sink and Apicurio converter
sql/              # DuckDB scripts for querying and exploring the lake
```

Data flow so far:

```
landing/trips/*.parquet -> ingest -> raw.trips -> Iceberg sink -> bronze.trips
                                         |
                                      cleaner -> clean.trips -> Iceberg sink -> silver.trips
                                         |            |
                                         |            +----> ClickHouse trips -> gold tables -> Grafana
                                         |
                                         +-----> dlq.trips (rejects, with a "rule" header)
```

The cleaner drops duplicate rows, joins zone names from `ref.zones`, and rejects reversals
(negative amounts), impossible durations or distances, and unknown zones.
`sql/explore.sql` shows how those rules were chosen. The Grafana dashboard "Xenny pipelines" shows
consumer lag and rejects by rule; "NYC taxi trips" shows the gold tables.

ArgoCD syncs `main` from GitHub, so changes take effect once pushed.

## Dev cluster (MacBook)

Requires Docker, k3d, kubectl, helm, Java 25 and Maven.

```sh
k3d cluster create --config bootstrap/k3d-dev.yaml
./bootstrap/install.sh
```

`install.sh` works against any cluster in the current kube context (e.g. k3s on the Ubuntu box).
It creates the secrets kept out of git, installs ArgoCD, and applies the root app.

Images (`apps/*`) are built by CI and pushed to GHCR (`ghcr.io/bryaneanders/xenny-*`) on push to `main`.
CI then commits the new `sha-<commit>` tag to the manifest that uses it, and ArgoCD deploys it.
Pull before your next commit to pick up that change.

## Ingesting a file

Upload a TLC trip file to `landing/trips/` (e.g. `yellow_tripdata_2026-07.parquet` from the
[TLC trip record page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)).
With the AWS CLI and the `s3` port-forward (see [Port-forwards](#port-forwards)):

```sh
export AWS_ACCESS_KEY_ID=$(kubectl -n pipelines get secret s3-credentials -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)
export AWS_SECRET_ACCESS_KEY=$(kubectl -n pipelines get secret s3-credentials -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)
aws --endpoint-url http://localhost:8333 s3 cp yellow_tripdata_2026-07.parquet s3://landing/trips/
```

The ingest service moves finished files to `processed/trips/` and unreadable ones to `failed/trips/`.

## Querying the lake

Tables are registered in the Lakekeeper catalog (warehouse `xenny`). Query them with DuckDB,
using the S3 keys from above and the `s3` and `lakekeeper` port-forwards.
New rows show up within about a minute, when the sink commits.

```sh
duckdb -c "
CREATE SECRET (TYPE s3, KEY_ID '$AWS_ACCESS_KEY_ID', SECRET '$AWS_SECRET_ACCESS_KEY',
               ENDPOINT 'localhost:8333', URL_STYLE 'path', USE_SSL false);
ATTACH 'xenny' AS lake (TYPE iceberg, URI 'http://localhost:8181/catalog',
                        AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none');
SELECT count(*) FROM lake.bronze.trips;"
```

`ACCESS_DELEGATION_MODE 'none'` makes DuckDB use its own S3 keys; the catalog doesn't hand out credentials.

## ClickHouse (gold)

`pipelines/trips/clickhouse.yaml` holds the trips DDL; a Job applies it after every sync.
`trips.trips_queue` reads `clean.trips` (consumer group `clickhouse-trips`), a materialized view moves
each batch into `trips.trips`, and `trips.zone_hourly` (trips and money per pickup zone per hour)
is recomputed from `trips.trips FINAL` every minute, so redelivered records never count twice.

Query it at http://localhost:8123/play with the `clickhouse` port-forward, as user `grafana` (read-only):

```sql
SELECT pickup_zone, sum(trips) AS trips FROM trips.zone_hourly GROUP BY pickup_zone ORDER BY trips DESC LIMIT 10
```

### Rebuilding ClickHouse

Kafka, not ClickHouse, remembers how far `trips_queue` has read. The data volume is kept if the
ClickHouse resources are deleted, so the two stay in step. If the data is lost anyway (or you drop
the `trips` database to rebuild it), reset the read position before the tables come back.
A group can only be reset while it has no consumers, so detach the queue first:

```sh
# In ClickHouse, as ingest: DETACH TABLE trips.trips_queue   (skip if the database is gone)
kubectl -n kafka exec xenny-dual-0 -c kafka -- bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --reset-offsets --to-earliest --group clickhouse-trips --topic clean.trips --execute
# Then ATTACH TABLE trips.trips_queue, or resync pipeline-trips to rerun the DDL Job.
```

The reset prints each partition's new offset; if it prints only the header, the group still had a consumer.

## Port-forwards

`tools/ports.sh` forwards the cluster's UIs and APIs to localhost and prints each URL and login.
Run it with no arguments to see the list and choose, or pass `all` or service names:

```sh
tools/ports.sh               # list, then choose
tools/ports.sh grafana s3    # just these
```

| Name | Local URL | What |
|---|---|---|
| argocd | https://localhost:8443 | ArgoCD UI |
| grafana | http://localhost:3000 | Grafana dashboards |
| prometheus | http://localhost:9090 | Prometheus |
| registry | http://localhost:8081 | Schema registry API, e.g. `/apis/ccompat/v7/subjects` |
| connect | http://localhost:8083 | Kafka Connect API, e.g. `/connectors?expand=status` |
| lakekeeper | http://localhost:8181/ui | Iceberg catalog UI; REST catalog at `/catalog` |
| s3 | http://localhost:8333 | SeaweedFS S3 API |
| filer | http://localhost:8888/buckets/ | SeaweedFS file browser |
| clickhouse | http://localhost:8123/play | ClickHouse SQL page |

Ctrl-C closes them all. A forward reconnects by itself if its pod restarts.
