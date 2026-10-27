'''

complex SQL queries for CitiBike Rebalancing Analysis API
Each function maps to one or more numbered queries

Joins:
  - trips / station_pairs use short_name for start/end station IDs
  - all other tables use station_id 
  '''
from db import query_db

# _____________________________
# FLAG QUERIES
# ______________________________

def get_flagged_stations(flag_type: str = None, severity: str = None, window: str = None, limit: int = 50)->list:
    '''
    Query 8: Rush-hour trouble stations, ranked by how long outages last.

    Filters (all optional): flag_type, severity, window (weekday_am_rush | weekday_pm_rush).
    Ranking: median_outage_minutes first (chronic_* only; NULLS LAST), then the size of
    metric_value, so imbalance_* rows (no outage minutes) rank by how strongly they drift.
    CAST(... AS text) gives Postgres a type for a NULL parameter, so "no filter" works
    with any driver.
    '''
    sql = '''
        SELECT
        s.station_id,
        s.station_name AS name,
        s.short_name,
        s.capacity,
        rf.flag_type,
        rf.window_label,
        rf.severity,
        rf.metric_value,
        rf.median_outage_minutes,
        rf.outage_count
    FROM rebalancing_flags rf
    JOIN stations s ON s.station_id = rf.station_id
    WHERE (CAST(:flag_type AS text) IS NULL OR rf.flag_type    = :flag_type)
      AND (CAST(:severity  AS text) IS NULL OR rf.severity     = :severity)
      AND (CAST(:window    AS text) IS NULL OR rf.window_label = :window)
    ORDER BY
        rf.median_outage_minutes DESC NULLS LAST,
        ABS(rf.metric_value)     DESC
    LIMIT :limit
    '''
    return query_db(sql, {'flag_type': flag_type, 'severity': severity,
                          'window': window, 'limit': limit})


def get_flag_summary()-> list:
    '''
    Query 9:Flag summary.

    Number of flagged stations per flag type, rush window and severity, plus the
    group's average metric_value. The table is rebuilt weekly, so every row is
    current. COUNT(*) equals the number of stations because rebalancing_flags is 
    UNIQUE on (station_id, flag_type, window_label).
    metric_value units depend on flag_type:
      chronic_*   -> % of rush snapshots empty/full
      imbalance_* -> net bikes gained(+)/lost(-) over the window, % of capacity
    '''
    sql = '''
    SELECT 
        flag_type,
        window_label,
        severity,
        COUNT(*) AS station_count,
        ROUND(AVG(metric_value), 1) AS avg_metric_value
    FROM rebalancing_flags
    GROUP BY flag_type, window_label, severity
    ORDER BY flag_type, window_label,
        CASE 
            WHEN severity = 'high' THEN 1
            WHEN severity = 'medium' THEN 2
            ELSE 3
        END;
        '''
    return query_db(sql)

# _____________________________________
# System overview 
# _____________________________________


def get_system_stats()->dict:
    '''
    Query 10: System-wide counts for the overview cards.

    Counts DISTINCT stations, not flag rows: one station can be flagged in both
    the AM and PM windows. Every row in rebalancing_flags is current because the
    weekly refresh rebuilds the table, so there is no "active" filter.
    flags_computed_at tells the viewer how fresh the flags are.
    '''
    
    sql = '''
        SELECT
            (SELECT COUNT(*) FROM stations) AS total_stations,
            (SELECT COUNT(*) FROM stations WHERE is_active) AS active_stations,
            (SELECT reltuples::bigint FROM pg_class WHERE relname = 'trips') AS total_trips,
            (SELECT reltuples::bigint FROM pg_class WHERE relname = 'availability_snapshots') AS total_snapshots,
            (SELECT COUNT(DISTINCT station_id) FROM rebalancing_flags) AS active_flags,
            (SELECT COUNT(DISTINCT station_id) FROM rebalancing_flags
                WHERE flag_type = 'chronic_empty') AS chronic_empty_count,
            (SELECT COUNT(DISTINCT station_id) FROM rebalancing_flags
                WHERE flag_type = 'chronic_full') AS chronic_full_count,
            (SELECT MAX(computed_at) FROM rebalancing_flags) AS flags_computed_at
        '''
    return query_db(sql, one_row=True)
    

def get_date_range()->dict:
    '''
    Query 11 Date range: MIN/MAX from trips and availability_snapshots
    ''' 
    sql = '''
    SELECT 
        (SELECT MIN(captured_at) FROM availability_snapshots) AS snapshots_starts,
        (SELECT MAX(captured_at) FROM availability_snapshots) AS snapshots_ends,
        (SELECT MIN(started_at) FROM trips) AS trips_start,
        (SELECT MAX(started_at) FROM trips) AS trips_end;
    '''
    return query_db(sql, one_row = True)

# ___________________________________________________
# At risk stations
# ___________________________________________________

def get_at_risk_stations(status: str = None, limit: int = 10) -> list:
    '''
    Query 13: At-risk now: stations empty, full, or close to it in the latest snapshot.

    Reads the technician_alerts view (defined in sql/schema.sql).
    Order: empty, full, near_empty, near_full; within a status
    the stations that are chronically empty/full during rush hour.'''
    sql = '''
        SELECT 
            station_id, 
            name, 
            short_name, 
            capacity,
            status, 
            bikes, 
            docks, 
            as_of,
            minutes_in_status,
            chronic_empty_pct, 
            chronic_full_pct
        FROM technician_alerts
        WHERE CAST(:status AS text) is Null or status = :status
        ORDER BY
            CASE 
                status WHEN 'empty' THEN 1 WHEN 'full' THEN 2
                WHEN 'near_empty' THEN 3 WHEN 'near_full' THEN 4
                ELSE 5
            END,
            GREATEST(COALESCE(chronic_empty_pct,0), COALESCE(chronic_full_pct,0)) DESC,
            minutes_in_status DESC NULLS LAST
        LIMIT :limit;
    '''
    return query_db(sql,{'status':status,'limit':limit})

def get_at_risk_counts() -> list:
    '''How many stations are in each at risk status right now.'''
    sql = '''
    SELECT status, COUNT(*) AS n
    FROM technician_alerts
    GROUP BY status
    '''
    return query_db(sql)
#_______________________________________
# Ranking Queries
# ______________________________________

def get_problem_stations_ranking(flag_type: str = None, window: str = None, per_group: int = 3)->list:
     '''
    Query 14: Problem stations ranked by the worst N per flag type and window.

    ''' 
     sql = '''
      WITH ordered_stations AS (
             SELECT *,
                 ROW_NUMBER() OVER (PARTITION BY flag_type, window_label
                                    ORDER BY median_outage_minutes DESC NULLS LAST,
                                             ABS(metric_value) DESC) AS rank_in_group,
                 COUNT(*) OVER (PARTITION BY flag_type, window_label) AS group_size
             FROM rebalancing_flags
            WHERE (CAST(:flag_type AS text) IS NULL OR flag_type = :flag_type)
                AND (CAST(:window AS text) IS NULL OR window_label = :window)
         )
         SELECT
             o.flag_type,
             o.window_label,
             o.rank_in_group,
             o.group_size,
             s.station_id,
             s.station_name AS name,
             s.short_name,
             o.severity,
             o.metric_value,
             o.median_outage_minutes
         FROM ordered_stations o
         JOIN stations s ON s.station_id = o.station_id
         WHERE o.rank_in_group <= :per_group
         ORDER BY o.flag_type, o.window_label, o.rank_in_group;
        '''

     return query_db(sql, {'flag_type':flag_type, 'window':window, 'per_group': per_group})

def get_worst_availability_stations()->list:
     '''
    Query 15: Stations with worst availability

   Takes every row in daily_station_metrics (one per day) and adds the averages two numbers:
                - the percentage of the day the station was empty
                - the percentage of the day it was full
    to get the percent of time a rider couldn't use this station
    - keeps the top 20 stations that are above the system average by day
    '''
     sql = '''
        SELECT 
            s.station_name AS name, 
            s.short_name,
            s.station_id,
            s.capacity,
            ROUND(AVG(d.pct_time_empty),2) AS avg_pct_empty,
            ROUND(AVG(d.pct_time_full),2) AS avg_pct_full,
            ROUND(AVG(d.pct_time_empty) + AVG(d.pct_time_full),2) AS avg_pct_unavailable,
            COUNT(*) AS days_sampled
        FROM daily_station_metrics d
        JOIN stations s ON s.station_id = d.station_id
        GROUP BY s.station_id, s.short_name, s.station_name, s.capacity
        HAVING AVG(d.pct_time_empty) + AVG(d.pct_time_full) >
                                (SELECT AVG(pct_time_empty + pct_time_full) FROM daily_station_metrics) --The average pct_unavailable
        ORDER BY  avg_pct_unavailable DESC
        LIMIT 20;
        '''
     return query_db(sql)

