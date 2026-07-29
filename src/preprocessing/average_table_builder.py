import sqlite3
import sys
from pathlib import Path
import pandas as pd 

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.helpers.loaders import load_table

DB_PATH = "data/db/tennis_abstract_new_version_merged_testing.db"

conn = sqlite3.connect(DB_PATH)
general_df = load_table(conn= conn, table_name = 'general').reset_index().drop(['player_name', 'ranking', 'matches_analyzed'], axis = 1)

mean_series = general_df.mean()

mean_df = mean_series.to_frame().T
mean_df = mean_df.apply(lambda x: round(x,2))
mean_df['player_name'] = 'ATP Average'
mean_df = mean_df.set_index('player_name')

mean_df.to_sql(name= 'global_averages', con=conn, if_exists='replace')


