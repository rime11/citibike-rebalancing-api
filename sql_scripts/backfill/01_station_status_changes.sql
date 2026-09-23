\timing on
BEGIN;

INSERT INTO station_status_changes
    (station_id, change_type, detected_at, value_before, value_after, snapshot_id)
WITH lagged AS (
    SELECT
        station_id,
        captured_at,
        snapshot_id,
        num_bikes_available,
        num_docks_available,
        is_renting,
        is_returning,
        LAG(num_bikes_available) OVER w AS prev_bikes,
        LAG(num_docks_available) OVER w AS prev_docks,
        LAG(is_renting)          OVER w AS prev_renting,
        LAG(is_returning)        OVER w AS prev_returning
    FROM availability_snapshots
    WINDOW w AS (PARTITION BY station_id ORDER BY captured_at, snapshot_id)
)
SELECT station_id, 'became_empty', captured_at,
       prev_bikes, num_bikes_available, snapshot_id
FROM lagged WHERE prev_bikes > 0 AND num_bikes_available = 0

UNION ALL
SELECT station_id, 'recovered_from_empty', captured_at,
       prev_bikes, num_bikes_available, snapshot_id
FROM lagged WHERE prev_bikes = 0 AND num_bikes_available > 0

UNION ALL
SELECT station_id, 'became_full', captured_at,
       prev_docks, num_docks_available, snapshot_id
FROM lagged WHERE prev_docks > 0 AND num_docks_available = 0

UNION ALL
SELECT station_id, 'recovered_from_full', captured_at,
       prev_docks, num_docks_available, snapshot_id
FROM lagged WHERE prev_docks = 0 AND num_docks_available > 0

UNION ALL
SELECT station_id, 'went_offline', captured_at,
       NULL::integer, NULL::integer, snapshot_id
FROM lagged
WHERE (prev_renting OR prev_returning)
  AND NOT is_renting AND NOT is_returning

UNION ALL
SELECT station_id, 'came_online', captured_at,
       NULL::integer, NULL::integer, snapshot_id
FROM lagged
WHERE NOT prev_renting AND NOT prev_returning
  AND (is_renting OR is_returning); 


CREATE INDEX idx_changes_station_time ON station_status_changes(station_id, detected_at);
CREATE INDEX idx_changes_type ON station_status_changes(change_type, detected_at);

COMMIT;
SELECT change_type, COUNT(*) FROM station_status_changes GROUP BY change_type ORDER BY 2 DESC;