# CitiBike Station Rebalancing Analysis

A data engineering project that identifies chronically problematic CitiBike stations to help operators rebalance bikes proactively rather than reactively.

**Live Demo:** [your-lightsail-ip:5000] | **Stack:** PostgreSQL · Flask · Python · AWS Lightsail

---

## The Problem

CitiBike operators physically move bikes between stations using trucks, which is an expensive, reactive process. Without data, dispatchers respond to problems after they happen. This system collects real-time availability data every 5 minutes and identifies stations that are *chronically* empty, full, or imbalanced, giving operators the intelligence to act before problems occur.

---

## Architecture

```
CitiBike GBFS API                               CitiBike trip data
station_information.json                        (monthly trip CSV)
station_status.json                                      |
        |                                                ▼ 
        ▼                                           python script
python scripy                                      (one time load)
(polled every 5 min)                                        |
        |                                                   |
        └───────────────────────┬───────────────────────────┘
                                ▼
                        Python ETL Script
                                |
                                ▼
                        PostgreSQL on AWS Lightsail
                        ┌─────────────────────────────┐
                        │  stations                   │
                        │  availability_snapshots     │
                        │  trips                      │
                        │  daily_station_metrics      │
                        │  hourly_patterns            │
                        │  station_pairs              │
                        │  station_status_changes     │
                        │  rebalancing_flags          │
                        │                             │
                        └─────────────────────────────┘
                                |
                                ▼
                        Flask API + Dashboard
```

---

## Data Scale

- ~2,400 stations across NYC
- ~9.6M availability snapshots collected over the project period
- ~3.8M historical trip records from CitiBike S3

---

## Features

**Flask Dashboard** (queries 8–15): System level analytics including network wide availability trends, worst offender stations by flag type, hourly demand heatmaps, and station flow summaries.

**REST API** (queries 1–7): Station specific endpoints for availability history, rebalancing flags, hourly patterns, and trip flow, demonstrated via Postman.

**Rebalancing Flags**: Data driven severity thresholds (low/medium/high at 10%/25%/50%) for three flag types:
- `chronic_empty`: stations that frequently run out of bikes during rushour
- `chronic_full`: stations that frequently have no open docks during rushour
- `high_imbalance`:  stations with large net trip outflows or inflows

---

## Database Schema

**Source tables** --> populated by ETL:
- `stations`: station metadata from the GBFS feed (~2,400 stations)
- `availability_snapshots`: bike/dock counts captured every 5 minutes (~9.6M rows)
- `trips`: historical trip records from CitiBike S3 (~3.8M rows)

**Precomputed summary tables** —-> aggregated at ETL time for dashboard performance:
- `daily_station_metrics`: by station, station daily rollups (trips started/ended, % time empty/full)
- `hourly_patterns`: average demand by hour-of-day(0 to 24) and day-of-week per station (0 for Sunday, 6 for Saturday)
- `station_pairs`: top origin-destination pairs by trip volume

**Analytical tables** —-> derived from the above:
- `rebalancing_flags`: stations with chronic operational problems, scored by severity
- `station_status_changes`: event driven log of state transitions (e.g., station goes offline)
---

## Local Setup

**Prerequisites:** PostgreSQL, Python 3.9+

1. Clone the repo
```bash
git clone https://github.com/rime11/citibike-rebalancing-api
```
2. Navigate to the project folder
```bash
cd citibike_api
```
3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Set up your environment variables
```bash 
cp .env.example .env
```
Then open `.env` and fill in your PostgreSQL credentials.

5. Create a database schema
```bash
psql -U your_user -d your_db -f ./sql/citibike_schema.sql
```

6. Run the app
```bash
python app.py
```

7. Open your browser and visit
```
Visit http://localhost:5000
```

---

## Data Sources

Trip data is not included in this repo due to file size. Download the zip file directly from the CitiBike S3 bucket:
```
https://s3.amazonaws.com/tripdata/index.html
```

Real-time availability is collected via the CitiBike GBFS API:
```
https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json
```

Station ID matching between the GBFS feed and historical CSVs is handled via `short_name`, resolving ~99% of stations without a separate mapping table.

---

## Project Structure

```
citibike-rebalancing/
├── app.py                  # Flask routes
├── db.py                   # Database connection
├── queries.py              # All 15 SQL queries
├── sql
      ├──citibike_schema.sql     # Full schema DDL
├── templates/              # Dashboard HTML
├── notebooks/              # Exploration and ETL development
├── .env.example
├── requirements.txt
└── README.md
```

---

## Key Technical Decisions

**Why `short_name` for ID matching?** The GBFS live feed and historical trip CSVs use different station ID formats. Rather than maintaining a separate mapping table, `short_name` resolves the mismatch at ~99% accuracy with zero additional overhead.

**Why precomputed summary tables?** `daily_station_metrics`, `hourly_patterns`, and `station_pairs` are computed once during ETL rather than at query time. At 9.6M snapshots, aggregating on the fly would make the dashboard unusable.

**Why data driven thresholds?** Flag severity cutoffs (10%/25%/50%) were derived from the actual distribution of `pct_time_empty` and `pct_time_full` across all stations rather than set arbitrarily, making them defensible and reproducible.

---

## Built With

- PostgreSQL (AWS Lightsail)
- Python — `psycopg2`, `pandas`, `Flask`, `SQLAlchemy`
- Postman (API testing)

---

*DTSC 691 Database Capstone — Eastern University*

