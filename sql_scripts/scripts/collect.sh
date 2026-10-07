
#!/bin/bash
set -e
/usr/bin/python3 /home/ubuntu/sql_scripts/backfill/fetch_snapshots.py
/usr/bin/python3 /home/ubuntu/sql_scripts/backfill/load_snapshots_data.py
                               
psql -d citibike -f sql_scripts/backfill/01_station_status_changes.sql  # all 9.6M snapshots → events

psql -d citibike -f sql/backfill/02_hourly_patterns.sql
psql -d citibike -f sql/backfill/03_daily_metrics.sql
psql -d citibike -f sql/backfill/04_flags.sql   