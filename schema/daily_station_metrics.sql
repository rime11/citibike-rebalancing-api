CREATE TABLE daily_station_metrics (
    metric_id            SERIAL PRIMARY KEY,
    station_id           VARCHAR(50) NOT NULL
                         REFERENCES stations(station_id) DEFERRABLE INITIALLY IMMEDIATE,
    summary_date         DATE NOT NULL,
    trips_started        INTEGER,          -- NULL = trips not yet published for this date
    trips_ended          INTEGER,
    net_flow             INTEGER GENERATED ALWAYS AS (trips_ended - trips_started) STORED,
    avg_bikes_available  DECIMAL(5,2),
    min_bikes_available  INTEGER,
    max_bikes_available  INTEGER,
    pct_time_empty       DECIMAL(5,2) CHECK (pct_time_empty BETWEEN 0 AND 100),
    pct_time_full        DECIMAL(5,2) CHECK (pct_time_full  BETWEEN 0 AND 100),
    computed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_daily_station_date UNIQUE (station_id, summary_date)
);
