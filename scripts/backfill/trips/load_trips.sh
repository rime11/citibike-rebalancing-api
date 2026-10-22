#!/bin/bash
# scripts/backfill/trips/load_trips.sh — usage: ./load_trips.sh 202606-citibike-tripdata.csv
set -e
FILE=$(realpath "$1")
REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
SQL_DIR="$REPO_ROOT/scripts/backfill/trips"

psql -d citibike <<EOF
\set ON_ERROR_STOP on
BEGIN;

CREATE TEMP TABLE trips_stage (
    ride_id            text,
    rideable_type      text,
    started_at         timestamp,
    ended_at           timestamp,
    start_station_name text,
    start_station_id   text,
    end_station_name   text,
    end_station_id     text,
    start_lat          numeric,
    start_lng          numeric,
    end_lat            numeric,
    end_lng            numeric,
    member_casual      text
) ON COMMIT DROP;

\copy trips_stage FROM '$FILE' WITH (FORMAT csv, HEADER true)

INSERT INTO trips (ride_id, rideable_type, started_at, ended_at,
                   start_station_id, start_station_name,
                   end_station_id, end_station_name,
                   start_lat, start_lng, end_lat, end_lng, member_casual)
SELECT DISTINCT ON (ride_id)
       -- CSV times are naive NYC local time
       ride_id, rideable_type,
       started_at AT TIME ZONE 'America/New_York', 
       ended_at   AT TIME ZONE 'America/New_York', 
       start_station_id, start_station_name,
       end_station_id, end_station_name,
       start_lat, start_lng, end_lat, end_lng, member_casual
FROM trips_stage
WHERE started_at IS NOT NULL
  AND ended_at   IS NOT NULL
  AND ended_at > started_at
ON CONFLICT (ride_id) DO NOTHING;

COMMIT;
EOF

echo "Loaded $FILE"

# trips changed -> rebuild the tables built from it, in dependency order

psql -d citibike -v ON_ERROR_STOP=1 -f "$SQL_DIR/hourly_patterns_refresh.sql"
psql -d citibike -v ON_ERROR_STOP=1 -f "$SQL_DIR/daily_metrics_refresh.sql"
echo "Rebuilt hourly_patterns and daily_station_metrics"