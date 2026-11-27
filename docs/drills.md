# Failure drills

Phase 6 breaks the platform on purpose to prove it recovers without losing or duplicating data.
Each drill below says what to do and what should happen. Afterwards, fill in its log: what
actually happened and what you changed. Every drill ends with `tools/reconcile.sh`, which must print
`PASS`.

| # | Drill | Alert it should fire |
|---|---|---|
| 1 | [A broker dies mid-ingest](#1-a-broker-dies-mid-ingest) | KafkaUnderReplicatedPartitions |
| 2 | [The cleaner dies mid-stream](#2-the-cleaner-dies-mid-stream) | |
| 3 | [Kafka Connect is down for an hour](#3-kafka-connect-is-down-for-an-hour) | KafkaConsumerLagHigh |
| 4 | [ClickHouse restarts during a load](#4-clickhouse-restarts-during-a-load) | |
| 5 | [Bad files](#5-bad-files) | TripsRejectRateHigh |
| 6 | [The Iceberg catalog goes down](#6-the-iceberg-catalog-goes-down) | KafkaConnectTaskFailed |
| 7 | [Schema evolution](#7-schema-evolution) | |
| 8 | [Backfill silver and gold](#8-backfill-silver-and-gold) | |

## Before you start

Start from a clean baseline: every ArgoCD app Synced and Healthy, and `tools/reconcile.sh` passing.

Each drill needs data moving through the pipeline, so it loads a month that isn't loaded yet.
Download a few spare months (the ones before your existing data are easiest):

```bash
export TRIPS=~/Documents/trip-data
for m in 2025-10 2025-11 2025-12 2026-01 2026-02; do
  curl -fL -o "$TRIPS/yellow_tripdata_$m.parquet" "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$m.parquet"
done
```

In one terminal, open the port-forwards and leave them running:

```bash
tools/ports.sh s3 lakekeeper registry connect clickhouse prometheus grafana
```

In a second terminal, at the repo root, set up these variables and helpers:

```bash
export TRIPS=~/Documents/trip-data
export AWS_ACCESS_KEY_ID=$(kubectl -n pipelines get secret s3-credentials -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)
export AWS_SECRET_ACCESS_KEY=$(kubectl -n pipelines get secret s3-credentials -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)
export CH_PASSWORD=$(kubectl -n warehouse get secret clickhouse-users -o jsonpath='{.data.grafana}' | base64 -d)
export CH_INGEST=$(kubectl -n warehouse get secret clickhouse-users -o jsonpath='{.data.ingest}' | base64 -d)

# Uploads a month to the landing bucket, e.g. upload 2026-02
upload() { aws --endpoint-url http://localhost:8333 s3 cp "$TRIPS/yellow_tripdata_$1.parquet" s3://landing/trips/; }
# Waits until ingest starts on the next file.
wait_ingest() { kubectl -n pipelines logs -f deploy/trips-ingest --since=1s | grep -m1 Ingesting; }
# Runs a Kafka CLI tool inside a broker pod, e.g. kafka kafka-topics --list
kafka() { kubectl -n kafka exec xenny-dual-0 -c kafka -- "bin/$1.sh" --bootstrap-server localhost:9092 "${@:2}"; }
# Total lag of a consumer group, e.g. lag trips-cleaner
lag() { kafka kafka-consumer-groups --describe --group "$1" 2>/dev/null | awk '$6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}'; }
# ClickHouse queries: read-only as grafana, or as ingest for the drills that change tables.
ch() { curl -sS -u "grafana:$CH_PASSWORD" http://localhost:8123/ --data-binary "$1 FORMAT PrettyCompact"; }
ch_ingest() { curl -sS -u "ingest:$CH_INGEST" http://localhost:8123/ --data-binary "$1"; }

# ArgoCD puts back anything changed by hand. To stop a component on purpose, turn off auto-sync for
# its app first (and for root, which would turn it back on). resume turns it all back on, and ArgoCD
# then undoes the manual changes.
pause() { for app in root "$1"; do kubectl -n argocd patch application "$app" --type json -p '[{"op":"remove","path":"/spec/syncPolicy/automated"}]'; done; }
resume() { kubectl apply -f bootstrap/root.yaml; }
```

Don't run `bootstrap/install.sh` while an app is paused: it reapplies the root app mid-drill.

Watch alerts at http://localhost:9090/alerts (an alert goes *pending*, then *firing* once its
`for:` time has passed) and on your phone through ntfy.

## 1. A broker dies mid-ingest

Tests that replication protects acknowledged data. Every topic has 3 replicas and
`min.insync.replicas=2`, and ingest sends with `acks=all` and idempotence, so a write only
succeeds once 2 brokers have it, and retries can't duplicate it.

**Part 1: kill a broker during an ingest.** Strimzi recreates the pod right away.

```bash
upload 2026-02
wait_ingest && kubectl -n kafka delete pod xenny-dual-2
kubectl -n kafka get pods -w          # Ctrl-C once xenny-dual-2 is Running again
kafka kafka-topics --describe --under-replicated-partitions   # empty once it has caught up
```

Any node works; `xenny-dual-2` keeps the `kafka` helper (which runs in `xenny-dual-0`) usable.

Expected:
- Ingest logs `Ingested trips/yellow_tripdata_2026-02.parquet` with the file's full row count. If a send
  times out, ingest retries the whole file, which shows up only as `bronze_dups`.
- The controller quorum keeps a leader throughout, since 2 of its 3 voters remain:
  `kafka kafka-metadata-quorum describe --status`.
- No under-replicated partitions a minute or two after the pod is back.

**Part 2: keep a broker down long enough to alert.** Strimzi's operator recreates deleted
pods, so stop it first:

```bash
pause strimzi
kubectl -n kafka scale deploy strimzi-cluster-operator --replicas 0
kubectl -n kafka delete pod xenny-dual-2
upload 2026-01        # optional: everything should keep working on 2 brokers
# Wait 5 minutes. KafkaUnderReplicatedPartitions should fire after 1.
resume                # ArgoCD restarts the operator, which recreates xenny-dual-2
```

Expected: with 2 of 3 brokers, every partition still has 2 in-sync replicas, so ingest, the cleaner
and the sinks carry on. The alert fires, then resolves once `xenny-dual-2` has caught up.

```bash
tools/reconcile.sh
```

**Log**
- Date:
- What happened:
- What I changed:

## 2. The cleaner dies mid-stream

Tests exactly-once processing. The cleaner reads `raw.trips`, updates its dedupe store and writes
`clean.trips` and `dlq.trips` in one Kafka transaction. If it dies mid-transaction, the transaction
is aborted, its writes are hidden from consumers that read only committed records, and the next
pod redoes that work.

```bash
upload 2026-01
wait_ingest; sleep 10; lag trips-cleaner      # non-zero: the cleaner is mid-file
kubectl -n pipelines delete pod -l app=trips-cleaner --grace-period=0 --force
kubectl -n pipelines logs -f deploy/trips-cleaner | grep -E 'State|Restor'   # Ctrl-C at RUNNING
```

`--grace-period=0 --force` doesn't give the cleaner time to finish its batch, which is closer to a
crash than a normal delete. The new pod starts with an empty disk and restores its dedupe store from
its changelog topic before it reads again.

Expected:
- The new pod goes `REBALANCING -> RUNNING` and `lag trips-cleaner` drops back to 0.
- No duplicates in `clean.trips`, silver or ClickHouse: reconcile passes, and this query
  shows 0 duplicates per file:

  ```bash
  duckdb -init sql/lake.sql -c "SELECT source_file, count(*) - count(DISTINCT row_number) AS duplicates FROM lake.silver.trips GROUP BY ALL ORDER BY ALL"
  ```

```bash
tools/reconcile.sh
```

**Log**
- Date: 2026-11-26 (by accident, during the cluster reload)
- What happened: loading 4 months at once, the cleaner hit its 1Gi memory limit and slowed to a
  crawl, then was OOMKilled mid-transaction.
  ClickHouse was exact, but silver had 7,029 duplicate July rows. The silver sink's consumer used
  Kafka's default `isolation.level=read_uncommitted`, so it kept the aborted copies as well as the
  ones the cleaner wrote again.
- What I changed: `consumer.isolation.level: read_committed` for all Connect sinks
  (`platform/connect/connect.yaml`). The cleaner got 2Gi of memory: at 1Gi, RocksDB had no file
  cache left and the cleaner slowed to a crawl (`pipelines/trips/cleaner.yaml`). The duplicates stay in
  silver until drill 8 rebuilds it.

**Log** (the planned run)
- Date:
- What happened:
- What I changed:

## 3. Kafka Connect is down for an hour

Tests that the Iceberg sinks pick up where they left off. They commit Kafka offsets together with
the Iceberg snapshot, so after an outage they neither skip nor repeat records.

```bash
pause connect
kubectl -n kafka patch kafkaconnect xenny --type merge -p '{"spec":{"replicas":0}}'
upload 2025-12
# Wait an hour. KafkaConsumerLagHigh should fire for connect-bronze-trips and connect-silver-trips
# 10 minutes after the file lands. Meanwhile the cleaner and ClickHouse keep up:
lag trips-cleaner; ch "SELECT count() FROM trips.trips FINAL WHERE source_file LIKE '%2025-12%'"
resume                # ArgoCD sets replicas back to 1
```

Expected: bronze and silver stop changing; gold stays current. After `resume`, Connect starts,
both sinks catch up within a few minutes, and the lag alert resolves.

```bash
tools/reconcile.sh
```

**Log**
- Date:
- What happened:
- What I changed:

## 4. ClickHouse restarts during a load

Tests that ClickHouse neither loses nor double-counts rows. ClickHouse commits Kafka offsets only
after a batch is stored, so a restart can deliver a batch twice. `trips.trips` is a
ReplacingMergeTree keyed on `(source_file, row_number)`, so the copies collapse, and reconcile
and gold read it with `FINAL`.

**Part 1: restart it mid-load.**

```bash
upload 2025-11
wait_ingest; sleep 5; ch "SELECT count() FROM trips.trips WHERE source_file LIKE '%2025-11%'"   # rising
kubectl -n warehouse delete pod chi-xenny-xenny-0-0-0
kubectl -n warehouse get pods -w      # Ctrl-C once it's Running
# Redelivered copies, before background merges remove them:
ch "SELECT (SELECT count() FROM trips.trips) - (SELECT count() FROM trips.trips FINAL) AS copies"
```

Expected: the pod comes back with the same volume and data; the month finishes loading; `copies`
may be above 0 for a while, but the `FINAL` counts are exact.

**Part 2: rebuild the schema from git.** Drop the trips database, rerun the DDL Job, and reload
from Kafka. `clean.trips` keeps 7 days, so this only recovers trips cleaned in the last week (all of
them, if the data was loaded this week); `dlq.trips` keeps everything.

```bash
ch "SELECT count() FROM trips.trips FINAL"            # note the total
ch_ingest "DROP DATABASE trips SYNC"
for g in clickhouse-trips:clean.trips clickhouse-rejects:dlq.trips; do
  kafka kafka-consumer-groups --reset-offsets --to-earliest --group "${g%%:*}" --topic "${g#*:}" --execute
done
# Sync the pipeline-trips app, which reruns the DDL Job (or press Sync in the ArgoCD UI).
kubectl -n argocd patch application pipeline-trips --type merge -p '{"operation":{"initiatedBy":{"username":"drill"},"sync":{}}}'
kubectl -n warehouse get jobs -w      # Ctrl-C once trips-clickhouse-schema is Complete
ch "SELECT count() FROM trips.trips FINAL"            # climbs back to the noted total
```

Expected: the reset prints each partition's new offset (it only works while nothing is consuming,
which dropping the database ensures). The Job recreates every table and view, both queues reload
from the start of their topics, and `zone_hourly` refills within a minute of the trips.

```bash
tools/reconcile.sh
```

**Log**
- Date:
- What happened:
- What I changed:

## 5. Bad files

Tests that bad input is contained: unreadable files go to `failed/`, and bad rows go to `dlq.trips`
with the rule they broke, instead of reaching silver or gold.

**Part 1: a file that isn't Parquet.**

```bash
echo "not a parquet file" > "$TRIPS/junk.parquet"
aws --endpoint-url http://localhost:8333 s3 cp "$TRIPS/junk.parquet" s3://landing/trips/
kubectl -n pipelines logs deploy/trips-ingest --since=2m | grep junk
aws --endpoint-url http://localhost:8333 s3 ls s3://landing/failed/trips/
```

Expected: ingest logs `trips/junk.parquet can't be read; moving to failed/`, and nothing reaches
Kafka.

**Part 2: a file whose rows all break a rule.** Make negative copies of 200,000 real trips, which
the cleaner rejects as reversals:

```bash
duckdb -c "COPY (SELECT * REPLACE (-abs(fare_amount) AS fare_amount, -abs(total_amount) AS total_amount)
                 FROM '$TRIPS/yellow_tripdata_2026-09.parquet' LIMIT 200000)
           TO '$TRIPS/reversals.parquet'"
aws --endpoint-url http://localhost:8333 s3 cp "$TRIPS/reversals.parquet" s3://landing/trips/
ch "SELECT rule, count() FROM trips.rejects FINAL WHERE source_file = 'trips/reversals.parquet' GROUP BY rule"
```

Expected: nearly all 200,000 rows land in `dlq.trips` with `rule=reversal`. The few left over have a
zero fare and total, so they're valid trips and pass. TripsRejectRateHigh fires about 2 minutes later
and resolves about 5 minutes after that.

```bash
tools/reconcile.sh
```

The file stays in bronze and the DLQ afterwards; reconcile counts it like any other file.

**Log**
- Date:
- What happened:
- What I changed:

## 6. The Iceberg catalog goes down

Not in the plan's list, but the only drill that makes a connector task fail. The sinks need the
Lakekeeper catalog to load tables and commit, so stopping it during a load should fail them.

```bash
pause lakekeeper
kubectl -n lake scale deploy lakekeeper --replicas 0
upload 2025-10
curl -s 'localhost:8083/connectors?expand=status' | jq -c '.[].status | {name, tasks: [.tasks[].state]}'
# Once a task is FAILED, KafkaConnectTaskFailed should fire after 1 minute.
resume                # ArgoCD scales Lakekeeper back up
# Strimzi may restart failed tasks itself; if a task is still FAILED:
for c in bronze-trips silver-trips; do curl -s -X POST "localhost:8083/connectors/$c/restart?includeTasks=true&onlyFailed=true"; done
```

Expected: the tasks fail within a commit interval (about a minute) of needing the catalog. ClickHouse
and gold carry on. After the restart, the sinks resume from their last committed offsets. Data files
written but never committed aren't part of the table, so there are no duplicates. If the tasks retry
instead of failing, write that down; it's a finding too, and the alert needs a different trigger.

```bash
tools/reconcile.sh
```

**Log**
- Date:
- What happened:
- What I changed:

## 7. Schema evolution

The registry checks every new version of a subject against the last one (`BACKWARD`: the new
schema must be able to read data written with the old one). Producers don't register schemas
(`auto-register` is off), so a schema only changes through git and `schemas/register.py`.

**Part 1: a compatible change.** Add an optional field to `clean.trips`, for example
`{"name": "trip_minutes", "type": ["null", "double"], "default": null}`, and roll it out.
Which goes first, the producer (the cleaner) or the consumers (silver, ClickHouse)?
With `BACKWARD` compatibility, consumers go first: they must be able to read the new field
before anything writes it.

1. Check the change before pushing:

   ```bash
   jq -n --rawfile s schemas/clean.trips-value.avsc '{schema: $s}' |
     curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data @- \
       localhost:8081/apis/ccompat/v7/compatibility/subjects/clean.trips-value/versions/latest
   ```

   Expected: `{"is_compatible":true}`.
2. Consumers. Silver needs nothing: the sink adds new fields to the Iceberg table by itself
   (`evolve-schema-enabled`). ClickHouse ignores Avro fields its tables don't have, so it needs
   the column on `trips.trips`. Its Kafka queue table also needs to be recreated, because
   ClickHouse can't alter that kind of table. Update `pipelines/trips/clickhouse.yaml` for fresh
   installs, and migrate the running tables by hand: add the column, then drop and recreate
   `trips_mv` and `trips_queue`. The read position is kept by Kafka, so nothing is lost.
3. Producer. Set the field in the cleaner (`Cleaner.toClean`) and add a test. Pushing the schema
   and the code together is fine: CI builds the new cleaner image after the push, so the schema is
   registered before the image that writes the field is deployed.
4. Check: new rows in silver and ClickHouse have `trip_minutes`, older rows have null, and reconcile
   passes.

**Part 2: a breaking change.** Add a required field (no default), which old data can't fill in.
Try registering it, as the register Job would, without pushing it:

```bash
tmp=$(mktemp -d)
jq '.fields += [{"name": "driver_id", "type": "string"}]' schemas/clean.trips-value.avsc > "$tmp/clean.trips-value.avsc"
REGISTRY_URL=http://localhost:8081 SCHEMA_DIR="$tmp" python3 schemas/register.py
```

Expected: `clean.trips-value: rejected (409) ...` and a non-zero exit. Pushed to git, the same
rejection fails the register Job and the schemas sync, and the cleaner can't write with an
unregistered schema. Either way, nothing reaches consumers. (Only run `register.py` this way with
a change you expect to be rejected: a compatible one would really be registered.)

**Log**
- Date:
- What happened:
- What I changed:

## 8. Backfill silver and gold

Rebuilds silver and gold from `raw.trips`, the way you'd apply a changed cleaner rule to past data.
The cleaner restarts from the beginning of `raw.trips` with an empty dedupe store, and silver and
ClickHouse are emptied first so they hold only its new output.

`raw.trips` keeps 7 days. For older files, follow the same steps, but reset the cleaner
`--to-latest` in step 4 and then replay the files as in the README's "Replaying files".

```bash
tools/reconcile.sh                      # note silver_total
pause pipeline-trips

# 1. Stop everything downstream of raw.trips.
kubectl -n pipelines scale deploy trips-cleaner --replicas 0
kubectl -n kafka patch kafkaconnector silver-trips --type merge -p '{"spec":{"state":"stopped"}}'
ch_ingest "DETACH TABLE trips.trips_queue"; ch_ingest "DETACH TABLE trips.rejects_queue"

# 2. Skip what's already in clean.trips and dlq.trips; the rebuilt tables get only new output.
for g in connect-silver-trips:clean.trips clickhouse-trips:clean.trips clickhouse-rejects:dlq.trips; do
  kafka kafka-consumer-groups --reset-offsets --to-latest --group "${g%%:*}" --topic "${g#*:}" --execute
done

# 3. Empty silver and the ClickHouse tables (the sink recreates silver.trips on its next write).
prefix=$(curl -s 'localhost:8181/catalog/v1/config?warehouse=xenny' | jq -r .defaults.prefix)
curl -sf -X DELETE "localhost:8181/catalog/v1/$prefix/namespaces/silver/tables/trips?purgeRequested=true"
ch_ingest "TRUNCATE TABLE trips.trips"; ch_ingest "TRUNCATE TABLE trips.rejects"

# 4. Reset the cleaner: its offsets go back to the start of raw.trips, and its internal topics
#    (the dedupe store's changelog) are deleted, so it starts with an empty store.
kafka kafka-streams-application-reset --application-id trips-cleaner --input-topics raw.trips --to-earliest

# 5. Start everything again.
ch_ingest "ATTACH TABLE trips.trips_queue"; ch_ingest "ATTACH TABLE trips.rejects_queue"
kubectl -n kafka patch kafkaconnector silver-trips --type json -p '[{"op":"remove","path":"/spec/state"}]'
resume                                  # ArgoCD scales the cleaner back to 1
```

`spec.state` is removed rather than set to `running`: git doesn't set it, so a value set by hand would
stay after `resume`.

Expected: the cleaner works through all of `raw.trips` (it did 9M records in about 2 minutes on
2026-11-26) until `lag trips-cleaner` is 0, and silver and ClickHouse fill back up. Reconcile passes with the same `silver_total` as before, minus any
duplicates the old silver had (such as drill 2's 7,029).

```bash
tools/reconcile.sh
```

**Log**
- Date:
- What happened:
- What I changed:

## Done when

From the plan's Phase 6 checklist:

- [ ] Restarting ClickHouse doesn't lose or double-count rows (drill 4)
- [ ] Gold totals match a direct count over silver (every reconcile)
- [ ] Deleting and recreating the ClickHouse resources rebuilds the schema from git (drill 4, part 2)
- [ ] Every drill ends with the reconciliation check passing
- [ ] A breaking schema change is rejected before it reaches consumers (drill 7)
- [ ] A full backfill of silver and gold completes and matches (drill 8)
- [ ] Each alert has fired at least once: KafkaUnderReplicatedPartitions (1), KafkaConsumerLagHigh (3),
      TripsRejectRateHigh (5), KafkaConnectTaskFailed (6)
