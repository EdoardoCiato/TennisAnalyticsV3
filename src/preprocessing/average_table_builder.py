import sys
from pathlib import Path
import pandas as pd 
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.data_access import get_general_table, upload_table

general_df = get_general_table().reset_index().drop(['player_name', 'ranking', 'matches_analyzed'], axis = 1)

mean_series = general_df.mean()

mean_df = mean_series.to_frame().T
mean_df = mean_df.apply(lambda x: round(x,2))
mean_df['player_name'] = 'ATP Average'
mean_df = mean_df.set_index('player_name')

upload_table(mean_df, 'global_averages')


