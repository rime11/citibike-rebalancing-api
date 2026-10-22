#!/bin/bash
set -euo pipefail
B=/home/ubuntu/citibike
/usr/bin/python3 $B/python/fetch_snapshots.py
/usr/bin/python3 $B/python/load_snapshots_data.py
$B/scripts/run_sql.sh jobs/detect_status_changes.sql   # last-60-minutes version, not the backfill