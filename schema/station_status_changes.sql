CREATE TABLE station_status_changes (
    change_id     SERIAL PRIMARY KEY,
    station_id    VARCHAR(50) NOT NULL
                  REFERENCES stations(station_id) DEFERRABLE INITIALLY IMMEDIATE,
    change_type   VARCHAR(30) NOT NULL,
    detected_at   TIMESTAMP   NOT NULL,
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
