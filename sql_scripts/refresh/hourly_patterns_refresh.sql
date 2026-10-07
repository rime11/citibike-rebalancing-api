\timing on
BEGIN;

TRUNCATE hourly_patterns; --truncate empties the table nothing time sensitive reads it, faster

INSERT INTO hourly_patterns (
    station_id, day_of_week, hour_of_day,
    avg_trips_started, avg_trips_ended, avg_net_flow, avg_bikes_available
)
WITH calendar AS (          -- how many Mondays, Tuesdays... are in the trip window
    SELECT EXTRACT(DOW FROM d)::int AS dow, COUNT(*) AS n_days
    FROM generate_series(
             (SELECT DATE(MIN(started_at)) FROM trips),
             (SELECT DATE(MAX(started_at)) FROM trips),
             interval '1 day'
         ) AS d
    GROUP BY 1
),
trip_starts AS (
    SELECT s.station_id,
           EXTRACT(DOW  FROM t.started_at)::int AS dow,
           EXTRACT(HOUR FROM t.started_at)::int AS hr,
           COUNT(*) AS total_starts
    FROM trips t
    JOIN stations s ON s.short_name = t.start_station_id
    WHERE t.started_at IS NOT NULL
    GROUP BY s.station_id, dow, hr
),
trip_ends AS (
    SELECT s.station_id,
           EXTRACT(DOW  FROM t.ended_at)::int AS dow,
           EXTRACT(HOUR FROM t.ended_at)::int AS hr,
           COUNT(*) AS total_ends
    FROM trips t
    JOIN stations s ON s.short_name = t.end_station_id
    WHERE t.ended_at IS NOT NULL
    GROUP BY s.station_id, dow, hr
),
availability_metrics AS (
    SELECT station_id,
           EXTRACT(DOW  FROM captured_at)::int AS dow,
           EXTRACT(HOUR FROM captured_at)::int AS hr,
           ROUND(AVG(num_bikes_available), 2) AS avg_bikes_available
    FROM availability_snapshots
    WHERE is_installed AND is_renting
    GROUP BY station_id, dow, hr
),
spine AS (
    SELECT station_id, dow, hr FROM trip_starts
    UNION
    SELECT station_id, dow, hr FROM trip_ends
    UNION
    SELECT station_id, dow, hr FROM availability_metrics
)
SELECT
    sp.station_id,
    sp.dow,
    sp.hr,
    ROUND(COALESCE(ts.total_starts, 0)::numeric / c.n_days, 2),
    ROUND(COALESCE(te.total_ends,   0)::numeric / c.n_days, 2),
    ROUND((COALESCE(te.total_ends, 0) - COALESCE(ts.total_starts, 0))::numeric / c.n_days, 2),
    am.avg_bikes_available
FROM spine sp
JOIN calendar c ON c.dow = sp.dow
LEFT JOIN trip_starts ts ON (ts.station_id, ts.dow, ts.hr) = (sp.station_id, sp.dow, sp.hr)
LEFT JOIN trip_ends te ON (te.station_id, te.dow, te.hr) = (sp.station_id, sp.dow, sp.hr)
LEFT JOIN availability_metrics am ON (am.station_id, am.dow, am.hr) = (sp.station_id, sp.dow, sp.hr);

COMMIT;