#!/bin/bash
# usage: run_sql.sh jobs/flags_refresh.sql
set -euo pipefail
BASE=/home/ubuntu/citibike
echo "$(date '+%F %T') start $1"
/usr/bin/psql -d citibike -v ON_ERROR_STOP=1 -q -f "$BASE/sql/$1"
echo "$(date '+%F %T') done  $1"