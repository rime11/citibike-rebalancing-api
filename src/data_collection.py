import requests
import json
from datetime import datetime
import time
import os


SNAPSHOT_DIR = "/home/ubuntu/data_collection" 
os.makedirs(SNAPSHOT_DIR, exist_ok=True) #creates folder if it doesn’t exist

#Describes the capacity and rental availability of a station
STATUS_URL = "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_status.json"
INFO_URL = "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_information.json"

#fetch station info at startup
info = requests.get(INFO_URL, timeout=30).json()
with open(f"{SNAPSHOT_DIR}/station_info.json", "w") as f:
    json.dump(info, f)
print(f"Saved station info. Starting collection loop...")

#loop forever
while True:
    try:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        status = requests.get(STATUS_URL, timeout=30).json()
        
        with open(f"{SNAPSHOT_DIR}/status_{ts}.json", "w") as f:
            json.dump(status, f)
        
        print(f"Captured {ts}")
    except Exception as e:
        print(f"Error at {datetime.utcnow()}: {e}")
    #wait 5 minutes 
    time.sleep(300)
