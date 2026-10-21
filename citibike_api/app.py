
'''
CitiBike Rebalancing Analysis API

Endpoints
/api/stations    # just a simple list 
/api/stations/<id>          # Query 1 
/api/stations/<id>/availability # Query 2 
/api/stations/<id>/metrics      # Query 3
/api/stations/<id>/hourly       # Query 4 

/api/stations/<id>/changes          # Query 7

/api/flags 	        # Query 8
/api/flags/summary   # Query 9 
/api/stats          # Query 10
/api/stats/range    # Query 11 

/api/corridors/imbalanced   # Query 13 
/api/rankings       # Query 14
/api/rankings/worst_availability       # Query 15
'''

#imports

from flask import Flask, jsonify, request, abort, render_template
from werkzeug.exceptions import HTTPException 
from dotenv import load_dotenv

import queries

load_dotenv()
app = Flask(__name__)


# ------------------------- Helpers -------------------------
def ok(data):
    """Success envelope used by every API route: {"status": "ok", "data": ...}."""
    return jsonify({"status": "ok", "data": data})

def _int_param(key, default, min_val=1, max_val=500):
    """Parse an integer query parameter, clamped to [min_val, max_val]; 400 if not an integer."""
    try:
        val = int(request.args.get(key, default))
    except (TypeError, ValueError):
        abort(400, description=f"'{key}' must be an integer")
    return max(min_val, min(max_val, val))

#----------------Error Handling----------------------------
@app.errorhandler(Exception)
def handle_exception(e):
    """Return JSON for every error.
    HTTP errors, show 404 from abort(), 400 from bad params.
    Anything else is unexpected: log the full traceback server side and send a
    generic message, so SQL and table names never reach the browser.
    """
    if isinstance(e, HTTPException):    
        return jsonify({"status": "error", "message": e.description}), e.code

    app.logger.exception("Unhandled error")   # message + full traceback to the log
    return jsonify({"status": "error", "message": "Internal server error"}), 500


#-------------------------Dashboard------------------------------
@app.get("/")
def dashboard():
    """Live dashboard, the JS on the page calls the /api routes."""
    return render_template("index.html")



#-------------------------Queries---------------------------------
#-----------------------STATION DETAIL QUERIES -------------------
@app.get("/api/stations")
def list_station_info():    
    """Returns a list of all stations with basic info"""
    rows = queries.get_station_info()
    
    return ok(rows)

@app.get("/api/stations/<station_id>")
def station_info(station_id):
    '''Query 1: Stations basic lookup with flags and status and recent trip activities in the last 7 days'''
    result = queries.get_station_info(station_id)
    if not result:
        abort(404, description = f"Station {station_id} not found")
    return ok(result)

@app.get("/api/stations/<station_id>/availability")
def latest_availability(station_id):
    '''Query 2: most recent bike and dock availability and availability_status for a specific station'''
         
    result = queries.get_latest_availability(station_id)
    if not result:
        abort(404, description = f"No availability for {station_id}")
    return ok(result)

@app.get("/api/stations/<station_id>/metrics")
def rolling_metrics(station_id):
    ''' 
    Query 3: moving average of trips started moving over past 7 days from current date, 
    and compare number of trips started to the previous day and print if it went down, up or stayed the same.
    '''
    rows = queries.get_rolling_metrics(station_id)
    if not rows:
        abort(404, description = f"No metrics for {station_id}")
    return ok(rows)
                    

@app.get("/api/stations/<station_id>/hourly")
def station_hourly(station_id):
    ''' 
    Query 4: Hourly_patterns for one station, weekday vs weekend.   
    This can be used to identify the peak hours for that station and how it changes by day of week. 
    '''
    rows = queries.get_hourly_demand_patterns(station_id)
    if not rows:
        abort(404, description = f"No hourly patterns for {station_id}")
    return ok(rows)


@app.get("/api/stations/<station_id>/changes")
def station_status_changes(station_id):
    '''
    Query 7: 
    
    20 most recent status changes with time since last change for a specific station,
    '''
    rows = queries.get_status_changes(station_id)
    if not rows:
        abort(404, description = f"Station {station_id} not found")
    return ok(rows)

#-------------------------Flags-------------------------
@app.get("/api/flags")
def flagged_stations():
    '''
    Query 8: Rush-hour trouble stations, ranked by median outage minutes.
    Query params (all optional):
      flag_type  chronic_empty | chronic_full | imbalance_toward_empty | imbalance_toward_full
      window     weekday_am_rush | weekday_pm_rush
      severity   high | medium | low
      limit      integer, default 50, max 500
    '''
    rows = queries.get_flagged_stations(
            flag_type=request.args.get("flag_type") or None,
            window=request.args.get("window") or None,
            severity=request.args.get("severity") or None,
            limit=_int_param("limit", default=50, max_val=500),
    )
    return ok(rows)

@app.get("/api/flags/summary")
def flag_summary():
    '''
    Query 9:Flag summary counts 
    returns a count of active and flagged stations by type and severity
    '''
    rows = queries.get_flag_summary()
    return ok(rows)

#-----------------------System overview----------------------
@app.get("/api/stats")
def system_stats():
    '''
    Query 10: System wide aggregate counts
    '''
    return ok(queries.get_system_stats())

@app.get("/api/stats/range")
def date_range():
    '''
    Query 11 Date range: MIN/MAX from trips and availability_snapshots
    '''

    return ok(queries.get_date_range())

@app.get('/api/at_risk')
def at_risk_stations():
    '''At-risk now: stations empty, full, or close to it in the latest snapshot (technician_alerts view).'''
    rows = queries.get_at_risk_stations(limit=_int_param("limit", default=10, max_val=200),
                                        status=request.args.get("status") or None)
    return ok(rows)

@app.get("/api/at_risk/summary")
def at_risk_summary():
    '''Count of stations per at-risk status.'''
    rows = queries.get_at_risk_counts()
    return ok(rows)

#-----------------------------Ranking Queries---------------------------
@app.get("/api/rankings")
def problem_station_ranking():
     '''
    Query 14: Unresolved problem stations ranked by flag type and severity
    '''
     rows = queries.get_problem_stations_ranking(flag_type=request.args.get("flag_type") or None,
                                                 window=request.args.get("window") or None,
                                                 per_group=_int_param("per_group", default = 3, max_val=20) 
                                                 )

     return ok(rows)

@app.get("/api/rankings/worst_availability")
def worst_availability():
     '''
    Query 15: Stations with worst availability
    '''
     rows = queries.get_worst_availability_stations()
     return ok(rows)

if __name__ == '__main__':
    app.run(debug=True, port = 5000)