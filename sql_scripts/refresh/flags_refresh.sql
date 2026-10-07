-- sql_scripts/refresh/flags_refresh.sql
-- Weekly. Rebuilds rebalancing_flags from:
--   availability_snapshots  -> how OFTEN a station is empty/full in rush hour (metric_value)
--   station_outages (view)  -> how LONG those outages last (median_outage_minutes, outage_count)
--   hourly_patterns         -> which WAY is the imbalance (imbalance_toward_empty / _full)
--
-- Full rebuild, not upsert: a station that stopped being a problem must lose its flag,
-- and ON CONFLICT DO UPDATE never deletes. DELETE (not TRUNCATE) so dashboard reads
-- keep seeing the old rows until COMMIT instead of blocking on a lock.
--
-- Assumes captured_at / started_at are New York local time.
 
BEGIN;
 
DELETE FROM rebalancing_flags; --dashboard reads it so it doesn't hang during a refresh.
 
WITH params AS (
    SELECT
        TIMESTAMPTZ '2026-01-15' AS date_range,
        --LOCALTIMESTAMP - INTERVAL '28 days' AS date_range, --only count last 4 weeks
        100    AS min_snapshots,       -- sample-size guard for chronic_* (per station-window)
        10.0   AS min_chronic_pct,     -- flag if empty/full in >= 10% of rush snapshots
        4      AS min_trip_days,       -- sample-size guard for imbalance_*
        50.0   AS min_imbalance_pct    -- flag if rush window drains/fills >= 50% of capacity
),
--rushhour windows AM 7-9, PM 5-7
windows (window_label, h_start, h_end) AS (
    VALUES ('weekday_am_rush', 7, 9),
           ('weekday_pm_rush', 17, 19)
),
 
-- How often: share of in service rush snapshots at 0 bikes / 0 docks.
-- Out of service snapshots are excluded so broken stations don't read as empty.
snap AS (
    SELECT
        s.station_id,
        w.window_label,
        COUNT(*) FILTER (WHERE s.is_installed IS TRUE AND s.is_renting IS TRUE)             AS n_renting,
        COUNT(*) FILTER (WHERE s.is_installed IS TRUE AND s.is_renting IS TRUE
                           AND s.num_bikes_available = 0)                                    AS n_empty,
        COUNT(*) FILTER (WHERE s.is_installed IS TRUE AND s.is_returning IS TRUE)           AS n_returning,
        COUNT(*) FILTER (WHERE s.is_installed IS TRUE AND s.is_returning IS TRUE
                           AND s.num_docks_available = 0)                                    AS n_full
    FROM availability_snapshots s
    CROSS JOIN params p
    JOIN windows w
      ON EXTRACT(HOUR FROM s.captured_at) BETWEEN w.h_start AND w.h_end
    WHERE s.captured_at >= p.date_range
      AND EXTRACT(ISODOW FROM s.captured_at) <= 5 --weekday
    GROUP BY s.station_id, w.window_label
),
chronic AS (
    SELECT station_id, window_label, 'chronic_empty' AS flag_type, 'empty' AS outage_type,
           100.0 * n_empty / n_renting AS pct
    FROM snap, params
    WHERE n_renting >= min_snapshots
    UNION ALL
    SELECT station_id, window_label, 'chronic_full', 'full',
           100.0 * n_full / n_returning
    FROM snap, params
    WHERE n_returning >= min_snapshots
),
 
-- How long: outages that START in the rush window. Excludes ongoing episodes
-- (duration truncated) and ones overlapping downtime (a van can't fix those).
outages AS (
    SELECT
        o.station_id,
        w.window_label,
        o.outage_type,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.minutes_out) AS median_minutes,
        COUNT(*) AS n_outages
    FROM station_outages o
    CROSS JOIN params p
    JOIN windows w
      ON EXTRACT(HOUR FROM o.started_at) BETWEEN w.h_start AND w.h_end
    WHERE o.outage_type IN ('empty', 'full')
      AND NOT o.is_ongoing
      AND NOT o.overlaps_offline
      AND o.started_at >= p.date_range
      AND EXTRACT(ISODOW FROM o.started_at) <= 5
    GROUP BY o.station_id, w.window_label, o.outage_type
),
 
-- Which way: average net bikes over the window per weekday, relative to capacity.
-- AM and PM are separate windows on purpose: a commuter station that drains in the
-- morning and refills in the evening nets to ~0 if you average them together.
-- Divides by 5 (not COUNT of rows) so hours with no trips count as zero flow.
imbalance AS (
    SELECT
        h.station_id,
        w.window_label,
        100.0 * (SUM(h.avg_net_flow) / 5.0) / st.capacity AS net_pct_of_capacity
    FROM hourly_patterns h
    JOIN windows  w  ON h.hour_of_day BETWEEN w.h_start AND w.h_end
    JOIN stations st ON st.station_id = h.station_id
    WHERE h.is_weekday
      AND st.capacity > 0
    GROUP BY h.station_id, w.window_label, st.capacity
    HAVING MIN(h.num_days_sampled) >= (SELECT min_trip_days FROM params)
)
 
INSERT INTO rebalancing_flags
    (station_id, flag_type, severity, metric_value,
     median_outage_minutes, outage_count, window_label)
SELECT
    c.station_id,
    c.flag_type,
    CASE WHEN c.pct >= 50 THEN 'high'
         WHEN c.pct >= 25 THEN 'medium'
         ELSE 'low' END,
    ROUND(c.pct, 2),
    ROUND(o.median_minutes::numeric, 1),
    COALESCE(o.n_outages, 0),
    c.window_label
FROM chronic c
CROSS JOIN params p
LEFT JOIN outages o
       ON o.station_id   = c.station_id
      AND o.window_label = c.window_label
      AND o.outage_type  = c.outage_type
WHERE c.pct >= p.min_chronic_pct
 
UNION ALL
 
SELECT
    i.station_id,
    CASE WHEN i.net_pct_of_capacity < 0 THEN 'imbalance_toward_empty'
         ELSE 'imbalance_toward_full' END,
    CASE WHEN ABS(i.net_pct_of_capacity) >= 100 THEN 'high'
         WHEN ABS(i.net_pct_of_capacity) >= 75  THEN 'medium'
         ELSE 'low' END,
    ROUND(i.net_pct_of_capacity, 2),
    NULL,
    NULL,
    i.window_label
FROM imbalance i
CROSS JOIN params p
WHERE ABS(i.net_pct_of_capacity) >= p.min_imbalance_pct;
 
COMMIT;
 