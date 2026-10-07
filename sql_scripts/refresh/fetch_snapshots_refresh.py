import os, sys, json, gzip, shutil
from datetime import datetime, timezone
from pathlib import Path
import requests, psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / '.env')
SNAPSHOT_DIR = Path("/home/ubuntu/sql_scripts/refresh")
ARCHIVE_DIR  = SNAPSHOT_DIR / "archive"
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
STATUS_URL = "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_status.json"
captured_at = datetime.now(timezone.utc)
ts   = captured_at.strftime("%Y%m%d_%H%M%S")
path = SNAPSHOT_DIR / f"status_{ts}.json"

try:
    status = requests.get(STATUS_URL, timeout=30).json()
    
    with open(f"status_{ts}.json", "w") as f:
        json.dump(status, f)
        
except Exception as e:
    print(f"Error at {datetime.now(timezone.utc)}: {e}")

###########database

DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSW')
}
#connect to database
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print('Connection Successful')
except Exception as e:
    print(f'Could not establish connection:{e}')
with open(f"status_{ts}.json", "r") as f:
    data = json.dump(f)
    stations = data['data']['stations']
    
    #iterate through the stations
    for station in stations:
        try:
            cur.execute("""
                INSERT INTO availability_snapshots 
                (station_id, captured_at, last_reported, num_bikes_available, 
                 num_ebikes_available, num_docks_available, num_bikes_disabled,
                 num_docks_disabled, is_installed, is_renting, is_returning)
                VALUES (%s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s)

            """, (
                station['station_id'],
                captured_at,
                station.get('last_reported'),
                station.get('num_bikes_available',0),
                station.get('num_ebikes_available', 0),
                station.get('num_docks_available',0),
                station.get('num_bikes_disabled', 0),
                station.get('num_docks_disabled', 0),
                bool(station.get('is_installed', 0)),
                bool(station.get('is_renting', 0)),
                bool(station.get('is_returning', 0))
            ))
        except Exception as e:
            print(f"Error in {f}, station {station['station_id']}: {e}")
            conn.rollback()
            continue
    
    conn.commit()