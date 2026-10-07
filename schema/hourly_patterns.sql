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
    computed_at          TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    CONSTRAINT uq_hourly_station_dow_hour UNIQUE (station_id, day_of_week, hour_of_day)
);
