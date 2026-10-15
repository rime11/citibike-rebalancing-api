
#imports
import os
from datetime import date, datetime
from decimal import Decimal
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL



#load .env variables
load_dotenv()
db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.environ["DB_USER"],
    password=os.environ["DB_PASSW"],   
    host=os.environ["DB_HOST"],
    port=int(os.environ["DB_PORT"]),
    database=os.environ["DB_NAME"]
)

 
#postgresql://user:password@host/dbname", connection pool min = 1 max = 3
#connects to database citibike on lightsail
engine = create_engine(db_url, pool_size=5,max_overflow=2,pool_pre_ping = True)      

def _to_json(value):
    """Make one column value safe for jsonify."""
    if isinstance(value, (datetime, date)):
        # ISO without a zone; Flask's default would label NY local time as GMT.
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value

def query_db(query, params=None,one_row=False):
    """Run a read only query.

    one_row=False -> list of dicts ([] when nothing matches)
    one_row=True  -> first row as a dict, or None when nothing matches
    Database errors propagate to the app's error handler, which logs them.
    """
     #run the queries
    
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        rows = [{k: _to_json(v) for k, v in row._mapping.items()} for row in result] #: _mapping is a dict-like view object 

    if one_row:
        return rows[0] if rows else None
    return rows
            
  

