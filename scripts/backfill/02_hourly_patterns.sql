-- per station, per day, per hour ==> avg_trips_started/ended net_flow(end_start) --> from trips
-- per station, per day, per hour avg_bikes_avail --> snapshots
-- runs monthly with flags
\timing on
BEGIN;

INSERT INTO hourly_patterns(
    station_id,
    day_of_week,
    hour_of_day,
    avg_trips_started,
    avg_trips_ended,
    avg_net_flow,
    avg_bikes_available,
    num_days_sampled
)
WITH trip_starts AS(
    SELECT s.station_id,
    EXTRACT(DOW FROM t.started_at)::INTEGER AS start_dow,
    EXTRACT(HOUR FROM t.started_at)::INTEGER AS start_hr,
    COUNT(*) AS total_starts --all the starts by station by day by hour
FROM trips t 
JOIN stations s on s.short_name = t.start_station_id
GROUP BY s.station_id, start_dow, start_hr
),
trip_ends AS(
    SELECT s.station_id,
    EXTRACT(DOW FROM t.ended_at)::INTEGER AS end_dow,
    EXTRACT(HOUR FROM t.ended_at)::INTEGER AS end_hr,
    COUNT(*) AS total_ends
FROM trips t 
JOIN stations s on s.short_name = t.end_station_id
GROUP BY s.station_id, end_dow,end_hr
),

availability_metrics AS(
SELECT
    station_id,
    EXTRACT(DOW FROM captured_at)::INTEGER AS dow,
    EXTRACT(HOUR FROM captured_at)::INTEGER AS hr,
    ROUND(AVG(num_bikes_available), 2) AS avg_bikes_available
FROM availability_snapshots
WHERE is_installed = true AND is_renting = true
GROUP BY station_id, dow, hr
),
all_tables AS(
SELECT station_id, start_dow AS dow, start_hr AS hr FROM trip_starts
UNION
SELECT station_id, end_dow AS dow, end_hr AS hr FROM trip_ends
UNION
SELECT station_id, dow,hr FROM availability_metrics
) --no duplicates
SELECT
    allt.station_id,--123/M/7am
    allt.dow AS day_of_week,
    allt.hr AS hour_of_day,
    -- divide by every calendar day of that weekday, not just days with trips
    ROUND(COALESCE(ts.total_starts, 0)::NUMERIC / c.n_days, 2) AS avg_trips_started,
    ROUND(COALESCE(te.total_ends, 0)::NUMERIC / c.n_days, 2) AS avg_trips_ended,
    ROUND((COALESCE(te.total_ends, 0) - COALESCE(ts.total_starts, 0))::NUMERIC / c.n_days, 2) AS avg_net_flow,
    am.avg_bikes_available,
    c.n_days AS num_days_sampled
FROM all_tables allt
JOIN (
    -- how many Sundays, Mondays, ... are in the trips data
    SELECT EXTRACT(DOW FROM d)::INTEGER AS dow, COUNT(*) AS n_days
    FROM generate_series(
        (SELECT DATE(MIN(started_at)) FROM trips),
        (SELECT DATE(MAX(started_at)) FROM trips),
        INTERVAL '1 day'
    ) AS d
    GROUP BY 1
) c ON c.dow = allt.dow
LEFT JOIN trip_starts ts ON ts.station_id = allt.station_id AND ts.start_dow = allt.dow AND ts.start_hr = allt.hr
LEFT JOIN trip_ends te ON te.station_id = allt.station_id AND te.end_dow = allt.dow AND te.end_hr = allt.hr
LEFT JOIN availability_metrics am ON am.station_id = allt.station_id AND am.dow = allt.dow AND am.hr = allt.hr;

COMMIT;