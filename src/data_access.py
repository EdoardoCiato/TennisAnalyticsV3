import sqlite3
import pandas as pd 
import sys
from pathlib import Path 
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.config import DB_PATH

def get_general_table() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query('SELECT * FROM GENERAL', con=conn)

def upload_table (df, name):
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(name, conn, if_exists='replace')