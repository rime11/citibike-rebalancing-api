# load_stations.py
import os, sys, json
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / '.env')

INFO_FILE = "/home/ubuntu/data_collection/station_info.json"

DB_CONFIG = {
    'host':     os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'user':     os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSW'),
}
with open(INFO_FILE) as f:
    stations = json.load(f)['data']['stations']

rows = [(
    s['station_id'],
    s.get('name'),
    s.get('short_name'),
    s.get('lat'),
    s.get('lon'),
    s.get('capacity'),
) for s in stations]

SQL = """
INSERT INTO stations (station_id, name, short_name, latitude, longitude, capacity,
                      is_active, last_updated)
VALUES %s
ON CONFLICT (station_id) DO UPDATE SET
    name         = EXCLUDED.name,
    short_name   = EXCLUDED.short_name,
    latitude     = EXCLUDED.latitude,
    longitude    = EXCLUDED.longitude,
    capacity     = EXCLUDED.capacity,
    is_active    = TRUE,
    last_updated = now()
"""
TEMPLATE = "(%s,%s,%s,%s,%s,%s,TRUE,now())"

DEACTIVATE = """
UPDATE stations
SET is_active = FALSE, last_updated = now()
WHERE is_active AND station_id <> ALL(%s)
"""
try:
    conn = psycopg2.connect(**DB_CONFIG)
except Exception as e:
    print(f"Could not connect: {e}")
    sys.exit(1)

with conn, conn.cursor() as cur:
    execute_values(cur, SQL, rows, template=TEMPLATE, page_size=1000)
    cur.execute(DEACTIVATE, ([s['station_id'] for s in stations],))
    print(f"Upserted {len(rows)}, deactivated {cur.rowcount}")

print(f"Loaded {len(rows)} stations")
conn.close()