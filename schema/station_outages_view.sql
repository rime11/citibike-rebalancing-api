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
 