import pandas as pd 
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.helpers.loaders import load_reference_table
from src.data_access import get_general_table, upload_table


# TODO: decide how to deal with rally aggressiveness and return aggressiveness because those 2 are already variation ( meaning that
#       0 should be the atp average as it is the mean of the circuit). How to normalize. 

# TODO: add ATP Average? Scale everything with the idea that the center is 0.5 with the ATP average values? 

def min_max( parity_dict):
    df = get_general_table()
    cols = df.columns
    info_cols = ['player_name', 'ranking', 'matches_analyzed']
    var_cols = list(set(cols)- set(info_cols))
    df[var_cols] = round((df[var_cols] - df[var_cols].min()) / (df[var_cols].max() - df[var_cols].min()) *100, 2)
    df[var_cols] = apply_parity(df[var_cols], parity_dict)
    df = df.set_index('player_name')
    upload_table(df, 'min_max_scaled')

def apply_parity(df, parity_dict):
    df_parity = df.copy()
    for indicator in df.columns:
        if parity_dict[indicator]['parity'] == -1:
            df_parity[indicator] = df[indicator].apply( lambda x: 100 -x )
    return df

def main():
    rows = load_reference_table()
    parity_dict = {row['column_name']: {"parity": row['parity'], "is_ratio": row['is_ratio'] }for row in rows} 
    min_max(parity_dict)

if __name__ == '__main__':
    main()


