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
        OR (median_outage_minutes IS NULL AND outage_count IS NULL)
);





