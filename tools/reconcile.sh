#!/usr/bin/env bash
# Checks that every ingested file's rows are accounted for in each layer:
#   file rows = bronze unique rows = silver + DLQ = ClickHouse trips, and gold total = silver total.
# Bronze stands in for raw.trips, which keeps only 7 days; extra bronze rows are ingest duplicates.
# Exits non-zero if any check fails. Needs duckdb and curl; starts its own port-forwards.
set -euo pipefail
cd "$(dirname "$0")/.."

secret() { kubectl -n "$1" get secret "$2" -o jsonpath="{.data.$3}" | base64 -d; }
export AWS_ACCESS_KEY_ID=$(secret pipelines s3-credentials AWS_ACCESS_KEY_ID)
export AWS_SECRET_ACCESS_KEY=$(secret pipelines s3-credentials AWS_SECRET_ACCESS_KEY)
CH_PASSWORD=$(secret warehouse clickhouse-users grafana)

# Reuse forwards already open (e.g. from tools/ports.sh); start the rest.
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
for f in lake/lakekeeper:8181 lake/seaweedfs-all-in-one:8333 warehouse/clickhouse-xenny:8123; do
  ns=${f%%/*} svc=${f#*/}; port=${svc##*:}; svc=${svc%:*}
  if ! (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    kubectl -n "$ns" port-forward "svc/$svc" "$port:$port" >/dev/null &
  fi
done
for port in 8181 8333 8123; do
  until (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; do sleep 1; done
done

# Per-file counts from ClickHouse, deduplicated with FINAL.
export CH_CSV=$(mktemp)
curl -sSf -u "grafana:$CH_PASSWORD" http://localhost:8123/ --data-binary "
  SELECT source_file, sum(dlq) AS dlq, sum(clickhouse) AS clickhouse,
         (SELECT sum(trips) FROM trips.zone_hourly) AS gold
  FROM (SELECT source_file, count() AS dlq, 0 AS clickhouse FROM trips.rejects FINAL GROUP BY source_file
        UNION ALL
        SELECT source_file, 0, count() FROM trips.trips FINAL GROUP BY source_file)
  GROUP BY source_file
  FORMAT CSVWithNames" >"$CH_CSV"

duckdb -box -init sql/lake.sql -c "
CREATE TEMP TABLE r AS
WITH files AS (
  SELECT 'trips/' || parse_filename(filename) AS source_file, count(*) AS file_rows
  FROM read_parquet('s3://landing/processed/trips/*.parquet', filename = true) GROUP BY ALL),
bronze AS (
  SELECT source_file, count(*) AS bronze, count(DISTINCT row_number) AS bronze_unique
  FROM lake.bronze.trips GROUP BY ALL),
silver AS (SELECT source_file, count(*) AS silver FROM lake.silver.trips GROUP BY ALL),
ch AS (FROM read_csv(getenv('CH_CSV')))
SELECT source_file, file_rows, bronze_unique, bronze - bronze_unique AS bronze_dups,
       silver, dlq, clickhouse, gold,
       file_rows = bronze_unique AND silver + dlq = bronze_unique AND clickhouse = silver AS ok
FROM files FULL JOIN bronze USING (source_file) FULL JOIN silver USING (source_file)
     FULL JOIN ch USING (source_file);

SELECT * EXCLUDE (gold) FROM r ORDER BY source_file;
SELECT sum(silver) AS silver_total, any_value(gold) AS gold_total, sum(silver) = any_value(gold) AS ok FROM r;
SELECT CASE WHEN bool_and(coalesce(ok, false)) AND sum(silver) = any_value(gold) THEN 'PASS'
            ELSE error('FAIL: see the rows marked false above') END AS reconciliation FROM r;
"
rm -f "$CH_CSV"
