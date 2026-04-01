import os
import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
#load .env variables
load_dotenv()


SNAPSHOT_DIR = "/home/ubuntu/data_collection"
DB_CONFIG = {
    'host': os.environ.get(DB_HOST),
    'database': os.environ.get(DB_NAME),
    'user': os.environ.get(DB_USER),
    'password': os.environ.get(DB_PASSW)
}
#connect to database
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print('Connection Successful')

except Exception as e:
    print(f'Could not establish connection:{e}')
#put files into a list
files = sorted([f for f in os.listdir(SNAPSHOT_DIR) if f.startswith('status_')])
print(f"Found {len(files)} files")

for i, filename in enumerate(files):
    # Parse timestamp from filename: status_20250115_143025.json
    ts_str = filename.replace('status_', '').replace('.json', '')
    captured_at = datetime.strptime(ts_str, '%Y%m%d_%H%M%S')
    #load json file
    with open(os.path.join(SNAPSHOT_DIR, filename)) as f:
        data = json.load(f)
    
    #extract stations in data field
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
            print(f"Error in {filename}, station {s['station_id']}: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    #for each 10 files print progress
    if (i + 1) % 100 == 0:
        print(f"Processed {i + 1}/{len(files)} files")

print("Done")
cur.close()
conn.close()