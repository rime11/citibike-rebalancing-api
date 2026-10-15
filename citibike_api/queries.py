'''
5,6,12,13
15 complex SQL queries for CitiBike Rebalancing Analysis API
Each function maps to one or more numbered queries

Joins:
  - trips / station_pairs use short_name for start/end station IDs
  - all other tables use station_id 
  '''
from db import query_db

# _____________________________________________
# STATION DETAIL QUERIES
# _____________________________________________

def get_station_info(station_id: str) -> dict:
    '''
Query 1: Station info: 

Stations basic lookup with flags and status and recent trip activities in the last 7 days. 
Left join stations to rebalancing flags table, since not every station will have a flag, 
use COALESCE function which replaces the no entry with none instead of throwing an error. 
:station_id is a placeholder
    '''
    sql = '''
    WITH trip_counts AS (
    SELECT start_station_id, COUNT(*) as trip_total
    FROM trips
    WHERE started_at >= (SELECT MAX(started_at) FROM trips) - INTERVAL '7 days'
    GROUP BY start_station_id
    )
    SELECT s.station_id,
        s.short_name,
        s.station_name,
        s.latitude,
        s.longitude,
        s.capacity,
        s.is_active,
        COALESCE(rf.flag_type,'none') AS flag_status,
        COALESCE(rf.severity,'none') AS flag_severity,
        COALESCE(tc.trip_total,0) AS trips_last_7_days
       
    FROM stations s
    LEFT JOIN rebalancing_flags rf ON rf.station_id = s.station_id
    LEFT JOIN trip_counts tc ON tc.start_station_id = s.short_name
    WHERE s.station_id = :station_id;
    '''
    return query_db(sql, {'station_id':station_id}, one_row = True)

def get_latest_availability(station_id:str)->dict:
    '''
Query 2:Latest availability: 

availability_snapshots is joined with stations, for most recent bike and dock availability and availability_status. 
Print station_id, name, num_bikes_available, num_docks_available, percent_full by dividing num_bikes_available by capacity 
and then print status if no bikes are available then print 'empty', no docks available then full or if bikes available is below 4 then low. 
'''
    sql = '''
    SELECT 
        a.station_id,
        s.station_name,
        a.num_bikes_available,
        a.num_ebikes_available,
        a.num_docks_available, 
        a.captured_at,
        s.capacity,
        CAST(100.0 * num_bikes_available /  NULLIF(s.capacity,0) AS DECIMAL(5,1)) AS pct_full,
        CASE
            WHEN a.num_bikes_available = 0 THEN 'empty'
            WHEN a.num_docks_available = 0 THEN 'full'
            WHEN a.num_bikes_available < 4 THEN 'low'
            ELSE 'normal'
        END AS availability_status
    FROM availability_snapshots a
    JOIN stations s ON a.station_id = s.station_id
    WHERE s.station_id = :station_id
    ORDER BY a.captured_at DESC 
    LIMIT 1;
    '''
    return query_db(sql, {'station_id':station_id}, one_row = True) 

def get_rolling_metrics(station_id:str)->list:
    '''
Query 3: Rolling 7-day metrics: 

Daily_station_metrics with window function for rolling average, from daily_station_metrics table. 
Print the station_id, date, trips_started, ended, net_flow, percent empty and full for that date, 
then print the moving average of trips started over past 7 days from current date, 
and compare number of trips started to the previous day and print if it went down, up or stayed the same. 
Limit the data to look at to the past 30 days 
    '''
    sql = '''
    SELECT
        station_id, 
        summary_date,
        trips_started,
        trips_ended, 
        net_flow,
        COALESCE(pct_time_empty, 0.0) AS pct_time_empty,
        COALESCE(pct_time_full, 0.0) AS pct_time_full,
        ROUND(AVG(trips_started) OVER (ORDER BY summary_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS trips_started_rolling_avg,
        LAG(trips_started,1) OVER (ORDER BY summary_date) AS pervious_day_starts,
        CASE
            WHEN trips_started > LAG(trips_started,1) OVER (ORDER BY summary_date) THEN 'up'
            WHEN trips_started < LAG(trips_started,1) OVER (ORDER BY summary_date) THEN 'down'
            ELSE 'same as day before'
        END AS trend
    FROM daily_station_metrics
    WHERE station_id = :station_id 
        AND summary_date >= (SELECT MAX(summary_date) FROM daily_station_metrics WHERE station_id = :station_id) - INTERVAL '30 days'
    ORDER BY summary_date DESC
    '''
    return query_db(sql, {'station_id':station_id})

def get_hourly_demand_patterns(station_id:str)->list:
    '''
    Query 4: Hourly demand patterns: 

    Hourly_patterns for one station, weekday vs weekend. 
    give the average trips started for a station at a specific hour for all weekdays/weekends, saturday = 6, sunday = 0 
    example for station z at 8am there are an average of 17 trips from monday to friday and 12 trips for saturday and sunday.
    '''
    sql = '''
    SELECT 
        hour_of_day,
        CASE 
            WHEN day_of_week BETWEEN 1 AND 5 THEN 'weekday'
            ELSE 'weekend' END AS weekday_weekend,
        ROUND(AVG(avg_trips_started),2) AS avg_trips_started,
        ROUND(AVG(avg_trips_ended),2) avg_trips_ended,
        ROUND(AVG(avg_net_flow),2) AS avg_net_flow
    FROM hourly_patterns
    WHERE station_id = :station_id
    GROUP BY hour_of_day,    
        CASE 
            WHEN day_of_week BETWEEN 1 AND 5 THEN 'weekday'
            ELSE 'weekend'
        END 
    ORDER BY hour_of_day, weekday_weekend;
    '''
    return query_db(sql, {'station_id':station_id})


def get_status_changes(station_id: str) -> list:
    '''
    Query 7: 
    
    20 most recent status changes for a station with time since last change, if the change went from 
    ('became_empty', 'became_full', 'went offline') to recovered,
    previous_value is the prev_num_bikes for became/recovered empty 
    and prev_num_docks for became/recovered full
    '''
    sql = '''
    SELECT 
        change_type,
        previous_value,
        detected_at,
        CAST(detected_at - LAG(detected_at) OVER (ORDER BY detected_at) AS TEXT) AS time_since_last_change,
        CASE 
            WHEN change_type IN ('became_empty', 'became_full', 'went offline') THEN 'problem'
            WHEN change_type IN ('recovered_from_full','recovered from empty','came online') THEN 'recovered'
            ELSE 'unknown'
        END AS event_category
    FROM station_status_changes
    WHERE station_id = :station_id
    ORDER BY detected_at DESC  
    LIMIT 20;
    '''
    return query_db(sql, {'station_id':station_id})

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
        ROUND(AVG(metric_value), 1) AS avg_metric_value,
        sum(outage_count) AS outage_count
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
            (SELECT COUNT(*) FROM stations WHERE is_active) AS flagged_stations,
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
    Order: empty, full, near_empty, near_full; within a status, longest outage first,
    then the stations that are chronically empty/full during rush hour.'''
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
        WHERE CAST(status AS text) is Null or status = :status
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
    '''How many stations are in each at-risk status right now.'''
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
    Query 14: Problem stations ranked by the worst 3 stations in each category.

    ''' 
     sql = '''
      WITH ordered_stations AS (
             SELECT *,
                 ROW_NUMBER() OVER (PARTITION BY flag_type, window_label
                                    ORDER BY median_outage_minutes DESC NULLS LAST,
                                             ABS(metric_value) DESC) AS rank_in_group,
                 COUNT(*) OVER (PARTITION BY flag_type, window_label) AS group_size
             FROM rebalancing_flags
            WHERE (CAST(:flag_type AS text) IS NULL OR flag_type    = :flag_type)
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

    This query finds the 20 worst performing stations by availability over the data. 
    For each station it calculates the average percentage of time spent empty, full, and 
    combines both into a single unavailability score. The HAVING clause filters out stations 
    that are performing at or below the system average, so only genuinely problematic stations make the list. 
    The result gives operations a ranked list of stations where bikes are either never there or never have space,
    ordered by whichever stations are unavailable the most.
    '''
     sql = '''
        SELECT 
            s.station_name AS name, 
            s.short_name,
            s.station_id,
            s.capacity,
            ROUND(AVG(dsm.pct_time_empty),2) AS avg_pct_empty,
            ROUND(AVG(dsm.pct_time_full),2) AS avg_pct_full,
            ROUND(AVG(dsm.pct_time_empty) + AVG(dsm.pct_time_full),2) AS avg_pct_unavailable,
            COUNT(*) AS days_sampled
        FROM daily_station_metrics dsm
        JOIN stations s ON s.station_id = dsm.station_id
        GROUP BY s.station_id, s.short_name, s.station_name, s.capacity
        HAVING AVG(dsm.pct_time_empty) + AVG(dsm.pct_time_full) >
                                (SELECT AVG(pct_time_empty + pct_time_full) FROM daily_station_metrics) --The average pct_unavailable
        ORDER BY  avg_pct_unavailable DESC
        LIMIT 20;
        '''
     return query_db(sql)

