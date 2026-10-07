-- Rebuild all derived tables + views. Keeps Source tables (stations, availability_snapshots, trips) untouched.
-- Run:  psql -d citibike -v ON_ERROR_STOP=1 -f schema/rebuild_derived_tbls.sql
-- Then run the backfills in sql/backfill/ (station_status_changes first, rebalancing_flags last).
BEGIN;
 
DROP VIEW  IF EXISTS station_outages;
DROP TABLE IF EXISTS rebalancing_flags, station_pairs, hourly_patterns,
                     daily_station_metrics, station_status_changes;
 

-- DERIVED TABLES (disposable: rebuilt from source tables)

 
CREATE TABLE station_status_changes (
    change_id     SERIAL PRIMARY KEY,
    station_id    VARCHAR(50) NOT NULL
                  REFERENCES stations(station_id) DEFERRABLE INITIALLY IMMEDIATE,
    change_type   VARCHAR(30) NOT NULL,
    detected_at   TIMESTAMPTZ   NOT NULL,
    value_before  INTEGER,        -- NULL for offline/online events
    value_after   INTEGER,        -- NULL for offline/online events
    snapshot_id   INTEGER
                  REFERENCES availability_snapshots(snapshot_id) DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT valid_change_type CHECK (change_type IN
        ('became_empty','became_full','recovered_from_empty',
         'recovered_from_full','went_offline','came_online')),
    -- one event of a given type per station per snapshot time; detection uses ON CONFLICT DO NOTHING
    CONSTRAINT uq_changes_event UNIQUE (station_id, change_type, detected_at)
);
CREATE INDEX "idx_changes_station_time" ON "station_status_changes" ("station_id", "detected_at");
CREATE INDEX "idx_changes_type" ON "station_status_changes" ("change_type", "detected_at");
COMMENT ON COLUMN "station_status_changes"."change_type" IS 'became_empty | became_full | recovered_from_empty | recovered_from_full | went_offline | came_online';

CREATE TABLE daily_station_metrics (
    metric_id            SERIAL PRIMARY KEY,
    station_id           VARCHAR(50) NOT NULL
                         REFERENCES stations(station_id) DEFERRABLE INITIALLY IMMEDIATE,
    summary_date         DATE NOT NULL,
    trips_started        INTEGER,          -- NULL = trips not yet published for this date
    trips_ended          INTEGER,           -- NULL = trips not yet published for this date
    net_flow             INTEGER GENERATED ALWAYS AS (trips_ended - trips_started) STORED,
    avg_bikes_available  DECIMAL(5,2),
    min_bikes_available  INTEGER,
    max_bikes_available  INTEGER,
    pct_time_empty       DECIMAL(5,2) CHECK (pct_time_empty BETWEEN 0 AND 100),
    pct_time_full        DECIMAL(5,2) CHECK (pct_time_full  BETWEEN 0 AND 100),
    computed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_daily_station_date UNIQUE (station_id, summary_date)
);


CREATE TABLE hourly_patterns (
    pattern_id           SERIAL PRIMARY KEY,
    station_id           VARCHAR(50) NOT NULL
                         REFERENCES stations(station_id) DEFERRABLE INITIALLY IMMEDIATE,
    day_of_week          INTEGER NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),  -- 0 = Sunday (EXTRACT(DOW))
    hour_of_day          INTEGER NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    is_weekday           BOOLEAN GENERATED ALWAYS AS (day_of_week BETWEEN 1 AND 5) STORED,
    avg_trips_started    DECIMAL(6,2),
    avg_trips_ended      DECIMAL(6,2),
    avg_net_flow         DECIMAL(6,2),
    avg_bikes_available  DECIMAL(5,2),
    num_days_sampled     INTEGER NOT NULL,  -- calendar days in window; sample size guard for high_imbalance
    computed_at          TIMESTAMPTZ NOT NULL DEFAULT LOCALTIMESTAMP,
    CONSTRAINT uq_hourly_station_dow_hour UNIQUE (station_id, day_of_week, hour_of_day)
);

CREATE TABLE station_pairs (
    pair_id               SERIAL PRIMARY KEY,
    start_station_id      VARCHAR(50) NOT NULL
                          REFERENCES stations(station_id) DEFERRABLE INITIALLY IMMEDIATE,
    end_station_id        VARCHAR(50) NOT NULL
                          REFERENCES stations(station_id) DEFERRABLE INITIALLY IMMEDIATE,
    trip_count            INTEGER NOT NULL CHECK (trip_count > 0),
    avg_duration_seconds  INTEGER,
    member_trips          INTEGER NOT NULL DEFAULT 0,
    casual_trips          INTEGER NOT NULL DEFAULT 0,
    computed_at           TIMESTAMPTZ NOT NULL DEFAULT LOCALTIMESTAMP,
    CONSTRAINT uq_pairs_start_end UNIQUE (start_station_id, end_station_id)
);
CREATE UNIQUE INDEX ON "station_pairs" ("start_station_id", "end_station_id");

CREATE TABLE rebalancing_flags (
    flag_id                SERIAL PRIMARY KEY,
    station_id             VARCHAR(50) NOT NULL
                           REFERENCES stations(station_id) DEFERRABLE INITIALLY IMMEDIATE,
    flag_type              VARCHAR(30) NOT NULL
                           CHECK (flag_type IN ('chronic_empty','chronic_full','imbalance_toward_empty','imbalance_toward_full')),
    severity               VARCHAR(10) NOT NULL CHECK (severity IN ('low','medium','high')),
    metric_value           NUMERIC(7,2) NOT NULL,  -- % of rush-hour snapshots (empty/full) or avg net flow (imbalance)
    median_outage_minutes  NUMERIC(7,1),           -- NULL for high_imbalance
    outage_count           INTEGER,                -- NULL for high_imbalance
    window_label           VARCHAR(40) NOT NULL 
                           CHECK (window_label IN ('weekday_am_rush','weekday_pm_rush')),   -- e.g. 'weekday_rush_last_28d'
    computed_at            TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,

    CONSTRAINT uq_flags_station_type_window UNIQUE (station_id, flag_type, window_label)

    CONSTRAINT chk_outage_cols_chronic_only CHECK (
        flag_type IN ('chronic_empty','chronic_full')
        OR (median_outage_minutes IS NULL AND outage_count IS NULL))
);

-- VIEW (compute on read; never refreshed)

CREATE OR REPLACE VIEW station_outages AS
WITH events AS (
    SELECT
        station_id,
        detected_at,
        CASE
            WHEN change_type IN ('became_empty', 'recovered_from_empty') THEN 'empty'
            WHEN change_type IN ('became_full',  'recovered_from_full')  THEN 'full'
            ELSE 'offline'                       -- went_offline / came_online
        END AS outage_type,
        change_type IN ('became_empty', 'became_full', 'went_offline') AS is_start
    FROM station_status_changes
),
next_event AS (
    --for each station's outage event find next event and timestamp
    SELECT
        station_id,
        outage_type,
        is_start,
        detected_at,
        LEAD(is_start)    OVER w AS next_is_start, --is the next event a start
        LEAD(detected_at) OVER w AS next_start
    FROM events
    WINDOW w AS (PARTITION BY station_id, outage_type ORDER BY detected_at)
),
episodes AS (
    -- pair each start with the next event of the same type; a start followed by
    -- another start (missed recovery, e.g. collector gap) is dropped
    SELECT
        station_id,
        outage_type,
        detected_at as started_at,
        next_start as ended_at, --will be null for ongoing
        next_is_start is NULL as is_ongoing
FROM next_event
WHERE is_start 
    AND (next_is_start IS NOT TRUE) -- filters for false and null values
)
SELECT
    e.station_id,
    e.outage_type,
    e.started_at,
    e.ended_at,
    e.is_ongoing,
    ROUND(EXTRACT(EPOCH FROM (COALESCE(e.ended_at, LOCALTIMESTAMP) - e.started_at)) / 60.0, 1)
                                                               AS minutes_out,
    -- empty/full episode that overlaps any offline episode: not a rebalancing problem
    e.outage_type <> 'offline' AND EXISTS (
        SELECT 1
        FROM episodes o
        WHERE o.station_id  = e.station_id
          AND o.outage_type = 'offline'
          -- does the outage overlap with offline
          AND o.started_at  < COALESCE(e.ended_at, LOCALTIMESTAMP)
          AND COALESCE(o.ended_at, LOCALTIMESTAMP) > e.started_at
    )  AS overlaps_offline
FROM episodes e;
 
COMMENT ON VIEW station_outages IS
  'One row per empty/full/offline episode, paired from station_status_changes. '
  'Ongoing episodes have ended_at NULL and minutes_out measured to now. '
  'overlaps_offline marks empty/full episodes contaminated by station downtime; exclude them from rebalancing metrics.';
 
COMMIT;
 