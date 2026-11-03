-- Data-quality exploration of bronze.trips, to choose the cleaner's validation rules.
-- Run one section at a time in a DuckDB shell. Start the shell with:
--   tools/ports.sh lakekeeper s3        (in another terminal)
--   duckdb sql/explore.duckdb -init sql/lake.sql
-- In a client connected to the lake some other way (e.g. DataGrip), open the local file first,
-- using its full path:
--   ATTACH IF NOT EXISTS '/path/to/xenny/sql/explore.duckdb' AS explore;
-- The data dictionary explains each column's codes:
--   https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

-- 0. Copy bronze to a local file once (about a minute). Every query below then runs locally in
--    milliseconds, and works without the cluster next time: `duckdb sql/explore.duckdb`.
CREATE OR REPLACE TABLE explore.main.trips AS FROM lake.bronze.trips;
CREATE OR REPLACE TABLE explore.main.zones AS
  FROM 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv';
USE explore.main;

-- 1. Shape: min, max, distinct count and null % of every column at once.
SUMMARIZE trips;

-- 2. Duplicates: Phase 2's restart duplicates share a key (file + row number).
SELECT count(*) AS rows, count(DISTINCT (source_file, row_number)) AS unique_keys FROM trips;

-- 3. Rows with mostly empty fields. Which payment type do they have? Look it up in the dictionary.
SELECT payment_type, count(*) AS rows,
       count(*) FILTER (WHERE passenger_count IS NULL) AS no_passenger_count,
       count(*) FILTER (WHERE ratecode_id IS NULL) AS no_ratecode
FROM trips GROUP BY ALL ORDER BY rows DESC;

-- 4. Pickups outside the month the file is named for.
SELECT source_file, count(*) AS rows, min(pickup_time), max(pickup_time),
       count(*) FILTER (WHERE strftime(pickup_time, '%Y-%m') <> regexp_extract(source_file, '(\d{4}-\d{2})', 1)) AS outside_month
FROM trips GROUP BY ALL ORDER BY 1;

-- 5. Trip duration: dropoff at or before pickup, and implausibly long trips.
SELECT count(*) FILTER (WHERE dropoff_time < pickup_time) AS dropoff_before_pickup,
       count(*) FILTER (WHERE dropoff_time = pickup_time) AS zero_duration,
       count(*) FILTER (WHERE dropoff_time - pickup_time > INTERVAL 6 HOUR) AS over_6_hours,
       count(*) FILTER (WHERE dropoff_time - pickup_time > INTERVAL 1 DAY) AS over_1_day
FROM trips;

-- 6. Distance (miles): zero, negative, and suspiciously long.
SELECT count(*) FILTER (WHERE trip_distance = 0) AS zero,
       count(*) FILTER (WHERE trip_distance < 0) AS negative,
       count(*) FILTER (WHERE trip_distance > 100) AS over_100_miles,
       max(trip_distance) AS longest
FROM trips;

-- Zero-distance trips: do they still have a fare? Did they start and end in the same zone?
SELECT pickup_location_id = dropoff_location_id AS same_zone, count(*) AS rows,
       round(avg(fare_amount), 2) AS avg_fare, round(avg(epoch(dropoff_time - pickup_time) / 60), 1) AS avg_minutes
FROM trips WHERE trip_distance = 0 GROUP BY ALL;

-- 7. Money: negative amounts. Are negative fares paired with a particular payment type?
SELECT payment_type, count(*) AS negative_fares, round(avg(total_amount), 2) AS avg_total
FROM trips WHERE fare_amount < 0 GROUP BY ALL ORDER BY 2 DESC;

SELECT count(*) FILTER (WHERE fare_amount = 0) AS zero_fare,
       count(*) FILTER (WHERE total_amount < 0) AS negative_total,
       count(*) FILTER (WHERE fare_amount > 500) AS fare_over_500,
       max(fare_amount) AS max_fare
FROM trips;

-- Does total_amount equal the sum of its parts? (Rounding allows a cent of slack.)
SELECT count(*) FILTER (WHERE abs(total_amount - (fare_amount + extra + mta_tax + tip_amount + tolls_amount
         + improvement_surcharge + coalesce(congestion_surcharge, 0) + coalesce(airport_fee, 0)
         + coalesce(cbd_congestion_fee, 0))) > 0.01) AS total_mismatch,
       count(*) AS rows
FROM trips;

-- 8. Passengers: zero, missing, or more than a cab holds.
SELECT passenger_count, count(*) AS rows FROM trips GROUP BY ALL ORDER BY 1 NULLS FIRST;

-- 9. Codes: values the data dictionary doesn't define.
SELECT 'vendor_id' AS col, vendor_id::VARCHAR AS value, count(*) AS rows FROM trips GROUP BY ALL
UNION ALL SELECT 'ratecode_id', ratecode_id::VARCHAR, count(*) FROM trips GROUP BY ALL
UNION ALL SELECT 'payment_type', payment_type::VARCHAR, count(*) FROM trips GROUP BY ALL
UNION ALL SELECT 'store_and_fwd_flag', store_and_fwd_flag, count(*) FROM trips GROUP BY ALL
UNION ALL SELECT 'request_source', request_source, count(*) FROM trips GROUP BY ALL
ORDER BY col, value NULLS FIRST;

-- 10. Zones: IDs missing from the lookup, and the lookup's own "unknown" zones (264, 265).
SELECT 'pickup' AS side, t.pickup_location_id AS id, z.Zone, count(*) AS rows
FROM trips t LEFT JOIN zones z ON z.LocationID = t.pickup_location_id
WHERE z.LocationID IS NULL OR z.Borough IN ('Unknown', 'N/A') GROUP BY ALL
UNION ALL
SELECT 'dropoff', t.dropoff_location_id, z.Zone, count(*)
FROM trips t LEFT JOIN zones z ON z.LocationID = t.dropoff_location_id
WHERE z.LocationID IS NULL OR z.Borough IN ('Unknown', 'N/A') GROUP BY ALL
ORDER BY rows DESC;

-- Negative fares are reversals: almost every one has a positive twin with the same times, zone and fare.
WITH neg AS (FROM trips WHERE fare_amount < 0)
SELECT count(*) AS negative_rows,
       count(*) FILTER (WHERE EXISTS (FROM trips p WHERE p.pickup_time = neg.pickup_time
         AND p.dropoff_time = neg.dropoff_time AND p.pickup_location_id = neg.pickup_location_id
         AND p.fare_amount = -neg.fare_amount)) AS with_positive_twin
FROM neg;

-- Total mismatches are systematic per vendor: vendor 1 also counts the congestion and CBD fees in
-- `extra`, and vendor 2's flex-fare rows leave congestion_surcharge null but still charge it.
-- So total_amount is right and the sum above is wrong.
WITH d AS (
  SELECT vendor_id, round(total_amount - (fare_amount + extra + mta_tax + tip_amount + tolls_amount
           + improvement_surcharge + coalesce(congestion_surcharge, 0) + coalesce(airport_fee, 0)
           + coalesce(cbd_congestion_fee, 0)), 2) AS diff
  FROM trips
)
SELECT vendor_id, diff, count(*) AS rows FROM d WHERE abs(diff) > 0.01 GROUP BY ALL ORDER BY rows DESC LIMIT 10;

-- 11. Putting it together: the cleaner's rules (apps/cleaner/.../Cleaner.java) and how many rows each rejects.
--     Zero distance, payment type 0, rate code 99 and zones 264/265 are all kept: they're real trips.
WITH checks AS (
  SELECT fare_amount < 0 OR total_amount < 0                     AS reversal,
         dropoff_time < pickup_time
           OR dropoff_time - pickup_time > INTERVAL 1 DAY        AS bad_duration,
         trip_distance < 0 OR trip_distance > 100                AS bad_distance,
         pickup_location_id NOT IN (FROM zones SELECT LocationID)
           OR dropoff_location_id NOT IN (FROM zones SELECT LocationID) AS unknown_zone
  FROM trips
)
SELECT count(*) AS rows,
       count(*) FILTER (WHERE reversal)     AS reversal,
       count(*) FILTER (WHERE bad_duration) AS bad_duration,
       count(*) FILTER (WHERE bad_distance) AS bad_distance,
       count(*) FILTER (WHERE unknown_zone) AS unknown_zone,
       count(*) FILTER (WHERE reversal OR bad_duration OR bad_distance OR unknown_zone) AS rejected,
       round(100.0 * rejected / rows, 2) AS rejected_pct
FROM checks;
