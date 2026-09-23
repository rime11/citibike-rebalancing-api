
--sql first run for daily_station_metrics table
INSERT INTO daily_station_metrics (
    station_id,
    summary_date,
    trips_started,
    trips_ended,
    avg_bikes_available,
    min_bikes_available,
    max_bikes_available,
    pct_time_empty,
    pct_time_full
)
with trip_starts AS(
       SELECT 
        s.station_id, 
        DATE(t.started_at) AS summary_date, 
        COUNT(*) AS trips_started
    FROM trips t
    JOIN stations s on t.start_station_id = s.short_name
    GROUP BY s.station_id, DATE(t.started_at)
),
trip_ends AS(
       SELECT 
        s.station_id, 
        DATE(t.ended_at) AS summary_date, 
        COUNT(*) AS trips_ended
    FROM trips t
    JOIN stations s on t.end_station_id = s.short_name
    GROUP BY s.station_id, DATE(t.ended_at)
),
availability AS (
        SELECT 
        station_id, 
        DATE(captured_at) AS summary_date,
        AVG(num_bikes_available) AS avg_bikes_available,
        MIN(num_bikes_available) AS min_bikes_available, 
        MAX(num_bikes_available) AS max_bikes_available, 
        100.0* SUM(CASE WHEN num_bikes_available = 0 THEN 1 else 0 END) / COUNT(*)  AS pct_time_empty,
        100.0 * SUM(CASE WHEN num_docks_available = 0 THEN 1 else 0 END) / COUNT(*)  AS pct_time_full
    FROM availability_snapshots
    GROUP BY station_id, DATE(captured_at)
)
SELECT
    COALESCE(s.station_id,e.station_id,av.station_id),  
    COALESCE(s.summary_date,e.summary_date, av.summary_date),
    COALESCE(s.trips_started,0),
    COALESCE(e.trips_ended,0),
    av.avg_bikes_available,
    av.min_bikes_available,
    av.max_bikes_available,
    av.pct_time_empty,
    Av.pct_time_full
FROM trip_starts s
FULL OUTER JOIN trip_ends e ON s.station_id = e.station_id 
    AND s.summary_date = e.summary_date
FULL OUTER JOIN availability av ON COALESCE(s.station_id,e.station_id) = av.station_id
    AND COALESCE(s.summary_date,e.summary_date) = av.summary_date;
