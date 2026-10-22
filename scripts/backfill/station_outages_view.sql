-- how long a station stayed empty/full, since it uses the data from station status it will be a view since it doesn't have new daata
--  stores when station became empty/full and when recovered in the same row from status_changes
-- the dashboard shows empty for how long

CREATE OR REPLACE VIEW station_outages AS
WITH events AS (
    SELECT
        station_id,
        detected_at,
        CASE WHEN change_type IN ('became_empty', 'recovered_from_empty')
             THEN 'empty' ELSE 'full' END              AS outage_type,
        change_type LIKE 'became\_%'                    AS is_start
    FROM station_status_changes
    WHERE change_type IN ('became_empty', 'recovered_from_empty',
                          'became_full',  'recovered_from_full')
),
sequenced AS (
    SELECT
        station_id,
        outage_type,
        is_start,
        detected_at,
        LEAD(is_start)    OVER w AS next_is_start,
        LEAD(detected_at) OVER w AS next_detected_at
    FROM events
    WINDOW w AS (PARTITION BY station_id, outage_type ORDER BY detected_at)
)
SELECT
    station_id,
    outage_type,
    detected_at                                         AS started_at,
    CASE WHEN NOT next_is_start THEN next_detected_at END AS ended_at,
    next_detected_at IS NULL                            AS is_ongoing,
    ROUND(EXTRACT(EPOCH FROM (
        COALESCE(CASE WHEN NOT next_is_start THEN next_detected_at END,
                 LOCALTIMESTAMP) - detected_at
    )) / 60.0, 1)                                       AS minutes_out
FROM sequenced
WHERE is_start
  AND (next_is_start = FALSE OR next_is_start IS NULL);

COMMENT ON VIEW station_outages IS
  'One row per empty/full episode, paired from station_status_changes. '
  'Ongoing outages have ended_at NULL and minutes_out measured to now.';