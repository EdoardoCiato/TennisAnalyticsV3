import pandas as pd 
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.helpers.loaders import load_reference_table

DB_PATH = "data/db/tennis_abstract_new_version_merged_testing.db"

# TODO: decide how to deal with rally aggressiveness and return aggressiveness because those 2 are already variation ( meaning that
#       0 should be the atp average as it is the mean of the circuit). How to normalize. 

# TODO: add ATP Average? Scale everything with the idea that the center is 0.5 with the ATP average values? 

def min_max(conn, parity_dict):
    df = pd.read_sql_query('SELECT * FROM general', conn)
    cols = df.columns
    info_cols = ['player_name', 'ranking', 'matches_analyzed']
    var_cols = list(set(cols)- set(info_cols))
    df[var_cols] = round((df[var_cols] - df[var_cols].min()) / (df[var_cols].max() - df[var_cols].min()) *100, 2)
    df_parity[var_cols] = apply_parity(df_parity[var_cols], parity_dict)
    df_parity = df_parity.set_index('player_name')
    df_parity.to_sql(name = 'min_max_scaled', con= conn, if_exists = 'replace')

def apply_parity(df, parity_dict):
    df_parity = df.copy()
    for indicator in df.columns:
        if parity_dict[indicator]['parity'] == -1:
            df_parity[indicator] = df[indicator].apply( lambda x: 100 -x )
    return df

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    rows = load_reference_table(cursor)
    parity_dict = {row['column_name']: {"parity": row['parity'], "is_ratio": row['is_ratio'] }for row in rows} 
    min_max(conn, parity_dict)

if __name__ == '__main__':
    main()


