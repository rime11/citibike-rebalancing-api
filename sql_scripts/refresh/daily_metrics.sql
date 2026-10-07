-- sql/jobs/daily_metrics_refresh.sql
BEGIN;

DELETE FROM daily_station_metrics
WHERE summary_date >= CURRENT_DATE - 2;  -- recompute the last 3 days; older history untouched

INSERT INTO daily_station_metrics (
    station_id, summary_date, trips_started, trips_ended,
    avg_bikes_available, min_bikes_available, max_bikes_available,
    pct_time_empty, pct_time_full
)
WITH availability AS (
    SELECT station_id,
           DATE(captured_at) AS summary_date,
           AVG(num_bikes_available) AS avg_bikes_available,
           MIN(num_bikes_available) AS min_bikes_available,
           MAX(num_bikes_available) AS max_bikes_available,
           -- out-of-service snapshots excluded, same rules as rebalancing_flags
           100.0 * COUNT(*) FILTER (WHERE is_installed IS TRUE AND is_renting IS TRUE AND num_bikes_available = 0)
                 / NULLIF(COUNT(*) FILTER (WHERE is_installed IS TRUE AND is_renting IS TRUE), 0)   AS pct_time_empty,
           100.0 * COUNT(*) FILTER (WHERE is_installed IS TRUE AND is_returning IS TRUE AND num_docks_available = 0)
                 / NULLIF(COUNT(*) FILTER (WHERE is_installed IS TRUE AND is_returning IS TRUE), 0) AS pct_time_full
    FROM availability_snapshots
    WHERE captured_at >= CURRENT_DATE - 2
    GROUP BY station_id, DATE(captured_at)
),
trip_starts AS (
    SELECT s.station_id, DATE(t.started_at) AS summary_date, COUNT(*) AS trips_started
    FROM trips t
    JOIN stations s ON t.start_station_id = s.short_name
    WHERE t.started_at >= CURRENT_DATE - 2
    GROUP BY s.station_id, DATE(t.started_at)
),
trip_ends AS (
    SELECT s.station_id, DATE(t.ended_at) AS summary_date, COUNT(*) AS trips_ended
    FROM trips t
    JOIN stations s ON t.end_station_id = s.short_name
    WHERE t.ended_at   >= CURRENT_DATE - 2
      AND t.started_at >= CURRENT_DATE - 3   -- lets Postgres use idx_trips_started_at
    GROUP BY s.station_id, DATE(t.ended_at)
),
trips_published AS (
    SELECT MAX(started_at)::date AS last_day FROM trips
)
SELECT a.station_id,
       a.summary_date,
       CASE WHEN a.summary_date <= tp.last_day THEN COALESCE(ts.trips_started, 0) END,  -- NULL = not yet published
       CASE WHEN a.summary_date <= tp.last_day THEN COALESCE(te.trips_ended,   0) END,
       a.avg_bikes_available,
       a.min_bikes_available,
       a.max_bikes_available,
       a.pct_time_empty,
       a.pct_time_full
FROM availability a
CROSS JOIN trips_published tp
LEFT JOIN trip_starts ts USING (station_id, summary_date)
LEFT JOIN trip_ends   te USING (station_id, summary_date);

COMMIT;