import os
import json
import psycopg2
from datetime import datetime

DB_HOST = 'localhost'
DB_NAME = 'citibike'
DB_USER = 'rime_my_user'
DB_PASSW = 'eastern_univ_user?!'
DB_PORT = 5432

SNAPSHOT_DIR = "/home/ubuntu/data_collection"
DB_CONFIG = {
    'host': DB_HOST,
    'database': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSW
}
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print('Connection Successful')

except Exception as e:
    print(f'Could not establish connection:{e}')

files = sorted([f for f in os.listdir(SNAPSHOT_DIR) if f.startswith('status_')])
print(f"Found {len(files)} files")

for i, filename in enumerate(files):
    # Parse timestamp from filename: status_20250115_143025.json
    ts_str = filename.replace('status_', '').replace('.json', '')
    captured_at = datetime.strptime(ts_str, '%Y%m%d_%H%M%S')
    
    with open(os.path.join(SNAPSHOT_DIR, filename)) as f:
        data = json.load(f)
    
    stations = data['data']['stations']
    
    for stan in stations:
        try:
            cur.execute("""
                INSERT INTO availability_snapshots 
                (station_id, captured_at, last_reported, num_bikes_available, 
                 num_ebikes_available, num_docks_available, num_bikes_disabled,
                 num_docks_disabled, is_installed, is_renting, is_returning)
                VALUES (%s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s)

            """, (
                stan['station_id'],
                captured_at,
                stan.get('last_reported'),
                stan.get('num_bikes_available',0),
                stan.get('num_ebikes_available', 0),
                stan.get('num_docks_available',0),
                stan.get('num_bikes_disabled', 0),
                stan.get('num_docks_disabled', 0),
                bool(stan.get('is_installed', 0)),
                bool(stan.get('is_renting', 0)),
                bool(stan.get('is_returning', 0))
            ))
        except Exception as e:
            print(f"Error in {filename}, station {s['station_id']}: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    #for each 10 print progress
    if (i + 1) % 100 == 0:
        print(f"Processed {i + 1}/{len(files)} files")

print("Done")
cur.close()
conn.close()