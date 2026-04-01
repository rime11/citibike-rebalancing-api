
'''
CitiBike Rebalancing Analysis API

Endpoints
/api/stations    # just a simple list 
/api/stations/<id>          # Query 1 
/api/stations/<id>/availability # Query 2 
/api/stations/<id>/metrics      # Query 3
/api/stations/<id>/hourly       # Query 4 
/api/stations/<id>/flows/outbound # Query 5 ⇒ outbound/inbound
/api/stations/<id>/flows/inbound   # Query 6
/api/stations/<id>/changes          # Query 7

/api/flags 	        # Query 8
/api/flags/summary   # Query 9 
/api/stats          # Query 10
/api/stats/range    # Query 11 
/api/corridors      # Query 12
/api/corridors/imbalanced   # Query 13 
/api/rankings       # Query 14
/api/rankings/worst_availability       # Query 15
'''

#imports
import os
from flask import Flask, jsonify, request, abort, render_template
from werkzeug.exceptions import HTTPException 
from dotenv import load_dotenv
import queries

load_dotenv()
app = Flask(__name__)

#----------------Error Handling----------------------------

@app.errorhandler(Exception)
def handle_exception(e):
    """Return JSON instead of HTML for ALL errors like 400, 404, 500 """
    # If it's a known Flask/HTTP error, use its code; otherwise, it's a 500
    code = 500
    if isinstance(e, HTTPException):
        code = e.code
    
    # Use your 'error' status for anything not a 200
    return jsonify({
        "status": "error",
        "message": str(e.description) if hasattr(e, 'description') else str(e)
    }), code

# lists/dicts wrapper so that we get json
@app.after_request
def wrap_success(response):
    """Automatically wrap lists/dicts in {'status': 'ok', 'data': dict/list}"""
    # Only wrap if it's a 200 OK and it's already JSON
    if response.status_code == 200 and response.is_json:
        data = response.get_json()
        
        # prevents double wrapping
        if isinstance(data, dict) and "status" in data:
            return response
            
        return jsonify({
            "status": "ok",
            "data": data
        })
    return response
#-------------------------Dashboard------------------------------
@app.get("/")
def dashboard():
    """dashboard page to test the API and show some visualizations"""
    return render_template("citibike_dashboard_prototype.html")

#-------------------------Helper---------------------------------
def _int_param(key, default, min_val=1, max_val=500):
    """parse an integer query parameter like limit"""
    try:
        val = int(request.args.get(key, default)) #limits the int to an int between min and max
        return max(min_val, min(max_val, val)) 
    except (TypeError, ValueError):
        abort(400, description=f"'{key}' must be an integer")

#-------------------------Queries---------------------------------
#-----------------------STATION DETAIL QUERIES -------------------
@app.get("/api/stations")
def list_stations():
    """Returns a list of all stations with basic info"""
    from db import query_db
    rows = query_db("SELECT station_id, short_name, name, latitude," \
                    "longitude, capacity, is_active " \
                    "FROM stations WHERE is_active = True " \
                    "ORDER BY name")
    
    return jsonify(rows)

@app.get("/api/stations/<station_id>")
def station_info(station_id):
    '''Query 1: Stations basic lookup with flags and status and recent trip activities in the last 7 days'''
    result = queries.get_station_info(station_id)
    if not result:
        abort(404, description = f"Station {station_id} not found")
    return jsonify(result)

@app.get("/api/stations/<station_id>/availability")
def latest_availability(station_id):
    '''Query 2: most recent bike and dock availability and availability_status for a specific station'''
         
    result = queries.get_latest_availability(station_id)
    if not result:
        abort(404, description = f"No availability for {station_id}")
    return jsonify(result)

@app.get("/api/stations/<station_id>/metrics")
def rolling_metrics(station_id):
    ''' 
    Query 3: moving average of trips started moving over past 7 days from current date, 
    and compare number of trips started to the previous day and print if it went down, up or stayed the same.
    '''
    rows = queries.get_rolling_metrics(station_id)
    if not rows:
        abort(404, description = f"No metrics for {station_id}")
    return jsonify(rows)
                    

@app.get("/api/stations/<station_id>/hourly")
def station_hourly(station_id):
    ''' 
    Query 4: Hourly_patterns for one station, weekday vs weekend.   
    This can be used to identify the peak hours for that station and how it changes by day of week. 
    '''
    rows = queries.get_hourly_demand_patterns(station_id)
    if not rows:
        abort(404, description = f"No hourly patterns for {station_id}")
    return jsonify(rows)

@app.get("/api/stations/<short_name>/flows/outbound")
def outbound_destinations(short_name):
    '''
    Query 5 and 6: Top outbound destinations and inbound origins: where do riders go from given station.
    Requires short_name because station_pairs uses short_name as its join key.
    Example: GET /api/stations/abc123/flows?short_name=6926.01
    '''
    rows =  queries.get_top_outbound_destinations(short_name)
    if not rows:
        abort(404, description = f"Station {short_name} not found or no flow data")
    return jsonify(rows)

@app.get("/api/stations/<short_name>/flows/inbound")
def inbound_origins(short_name):
    '''
    Query 6: Top inbound destinations:where do riders come from to given station
    Requires short_name because station_pairs uses short_name as its join key.
    Example: GET /api/stations/abc123/flows?short_name=6926.01
    '''
    rows = queries.get_top_inbound_origins(short_name)
    if not rows:
        abort(404, description = f"Station {short_name} not found or no flow data")
    return jsonify(rows)

@app.get("/api/stations/<station_id>/changes")
def station_status_changes(station_id):
    '''
    Query 7: 
    
    20 most recent status changes with time since last change for a specific station,
    '''
    rows = queries.get_status_changes(station_id)
    if not rows:
        abort(404, description = f"Station {station_id} not found")
    return jsonify(rows)

#-------------------------Flags-------------------------
@app.get("/api/flags")
def flagged_stations():
    '''
    Query 8: Active flagged stations with optional filters
    Query params:
      flag_type  chronic_empty | chronic_full | high_imbalance  
      severity   high | medium | low                            
      limit      integer, default 50, max 500 
    '''
    flag_type = request.args.get('flag_type') or None
    severity = request.args.get('severity') or None
    limit = _int_param('limit', default = 50, max_val = 500)

    rows = queries.get_flagged_stations(flag_type=flag_type,severity=severity, limit=limit)
    return jsonify(rows)

@app.get("/api/flags/summary")
def flag_summary():
    '''
    Query 9:Flag summary counts 
    returns a count of active and flagged stations by type and severity
    '''
    rows = queries.get_flag_summary()
    return jsonify(rows)

#-----------------------System overview----------------------
@app.get("/api/stats")
def system_stats():
    '''
    Query 10: System wide aggregate counts
    '''
    result = queries.get_system_stats()
    return jsonify(result)

@app.get("/api/stats/range")
def date_range():
    '''
    Query 11 Date range: MIN/MAX from trips and availability_snapshots
    '''
    result = queries.get_date_range()
    return jsonify(result)

#---------------------Corridor----------------------------
@app.get("/api/corridors")
def busiest_corridor():
    '''
    Query 12: Tops Busiest station_pairs corridors and imbalance
    Query params:
      min_trips  minimum trip count filter, default = 500
      limit      max rows for each query, default = 20, max = 100
    '''
    min_trips = _int_param('min_trips', default = 500, min_val=1, max_val=99999)
    limit = _int_param('limit', default=20, min_val=1, max_val=500)
    rows = queries.get_busiest_corridor(min_trips= min_trips, limit = limit)
    return jsonify(rows)

@app.get("/api/corridors/imbalance")
def biderectional_imbalance():
     '''
    Query 13: Bidirectional trip imbalance
    Query params:
    limit default = 20 max = 500
    '''
     limit = _int_param('limit',default=20, min_val=1, max_val=500)
     rows= queries.get_bidirectional_trip_imbalance(limit=limit)
     return jsonify(rows)

#-----------------------------Ranking Queries---------------------------
@app.get("/api/rankings")
def problem_station_ranking():
     '''
    Query 14: Unresolved problem stations ranked by flag type and severity
    '''
     rows = queries.get_problem_stations_ranking()
     return jsonify(rows)

@app.get("/api/rankings/worst_availability")
def worst_availability():
     '''
    Query 15: Stations with worst availability
    '''
     rows = queries.get_worst_availability_stations()
     return jsonify(rows)

if __name__ == '__main__':
    app.run(debug=True, port = 5000)