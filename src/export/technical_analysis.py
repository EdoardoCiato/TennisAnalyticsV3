import sys
from pathlib import Path
import pandas as pd
import sqlite3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.export.radar_chart import radar_chart
from src.config import DB_PATH

RADAR_INDICATORS = [
    'Serve Games hold %','Break Pts Saved',
    'Break %', 'Games with Break Pts',
    'Winners', 'Unforced Errors'
]

PLAYERS = [
    'MatteoArnaldi', 'LorenzoSonego'
]

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    df = pd.read_sql_query('Select * from GENERAL ', conn)
    full_df = df.set_index('player_name')
    scaled_full_df = (full_df - full_df.min()) / (full_df.max() - full_df.min())  
    df = scaled_full_df.loc[PLAYERS, RADAR_INDICATORS].apply(lambda x: x*100).reset_index()
    print(df)
    player1 = PLAYERS[1]
    player2 = PLAYERS[0]
    path = radar_chart(df,  player1, player2, RADAR_INDICATORS, 'technical')

main()