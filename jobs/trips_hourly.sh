#!/bin/bash
set -euo pipefail
BASE=/home/ubuntu/citibike
/usr/bin/python3 $BASE/python/load_trips.py "$1"             # e.g. 202609-citibike-tripdata.csv
$BASE/scripts/run_sql.sh jobs/hourly_patterns_refresh.sql 