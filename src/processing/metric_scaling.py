import pandas as pd
def min_max_scaling( cursor, players, df):
    cursor.execute('SELECT * FROM GENERAL')
    all_rows = cursor.fetchall()
    full_df = pd.DataFrame(data=all_rows, columns=[desc[0] for desc in cursor.description])
    full_df = full_df.set_index('player_name')
    scaled_full_df = (full_df - full_df.min()) / (full_df.max() - full_df.min())
    selected_players =  players
    scaled_report_df = scaled_full_df.loc[selected_players].T
    indicators_of_category = list(df.index)
    scaled_category_report = scaled_report_df.loc[indicators_of_category]
    return scaled_category_report

def apply_parity(df, parity_dict):
    for indicator in df.index:
        if parity_dict[indicator]['parity'] == -1:
            df.loc[indicator] = 1 -  df.loc[indicator]
    return df

def compute_delta(df, players):
    p1 = df[players[0]].copy()
    p2 = df[players[1]].copy()
    return p2 - p1





