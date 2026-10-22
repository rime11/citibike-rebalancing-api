
#!/bin/bash
set -euo pipefail
BASE=/home/ubuntu/citibike
PSQL="/usr/bin/psql -d citibike -v ON_ERROR_STOP=1"

echo "$(date) fetching + loading snapshots"
/usr/bin/python3 $BASE/python/backfill/fetch_snapshots.py
/usr/bin/python3 $BASE/python/backfill/load_snapshots_data.py

echo "$(date) status changes"
$PSQL -f $BASE/backfill/01_station_status_changes.sql

echo "$(date) hourly patterns"
$PSQL -f $BASE/backfill/02_hourly_patterns.sql

echo "$(date) daily metrics"
$PSQL -f $BASE/backfill/03_daily_metrics.sql

echo "$(date) flags"
$PSQL -f $BASE/backfill/04_flags.sql

echo "$(date) done" 