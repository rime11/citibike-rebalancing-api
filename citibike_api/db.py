
#imports
import os
from sqlalchemy import create_engine, text
import psycopg2
from sqlalchemy.exc import SQLAlchemyError 
from dotenv import load_dotenv
#load .env variables
load_dotenv()

#postgresql://user:password@host/dbname", connection pool min = 1 max = 3
#connects to database citibike on lightsail
engine = create_engine(f"postgresql://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSW')}"
                       f"@{os.environ.get('DB_HOST')}:{os.environ.get('DB_PORT')}/{os.environ.get('DB_NAME')}",
                         pool_size=3,
                         max_overflow=0,
                         pool_pre_ping = True)
                   

def query_db(query, params=None, one_row = False):
     #run the queries
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            rows = [row._asdict() for row in result]
            return rows[0] if one_row else rows
    except SQLAlchemyError as e:
        raise RuntimeError(f'Error: {e}') from e
       

