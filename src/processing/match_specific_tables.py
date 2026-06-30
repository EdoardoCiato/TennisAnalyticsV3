import pandas as pd
import sqlite3
from src.processing.metric_scaling import min_max_scaling, compute_delta, apply_parity
from src.export.excel_exporter import export_tables_by_category
from src.helpers.helper_functions import create_category_dictionary
from src.helpers.helper_functions import load_reference_table

def table_exists(cur, name):
    # function for sanity check to check that all the needed tables exist. 
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def pull_average_values(cursor, indicators):
    columns = ','.join(f'"{col}"' for col in indicators)
    cursor.execute(f'''
    SELECT {columns} FROM global_averages LIMIT 1''')
    avg_row = cursor.fetchone()

    if avg_row is None:
        return []
    return [avg_row]

def pull_table_values(cursor, indicators, players):
    if not indicators:
        return pd.DataFrame(columns=["Player"])
    if not players:
        return pd.Dataframe(columns=indicators)
    
    columns = ','.join(f'"{col}"' for col in indicators)
    placeholders = ','.join(f"?" for _ in players)
    query = f'''
    SELECT {columns} FROM general WHERE "player_name" in ({placeholders})'''
    cursor.execute(query, players,)
    players_rows = cursor.fetchall()
    avg_row = pull_average_values(cursor, indicators)
    all_rows = avg_row + players_rows
    df = pd.DataFrame(data = all_rows, columns = indicators)
    df = df.set_index('player_name').T
    df.index.name = 'indicator'
    df.columns.name = 'player'
    return df

def delta_col_and_sorting( df, parity_dict, cursor, players ):
    df_scaled = df.copy()
    df_scaled = min_max_scaling(cursor, players, df)
    df_scaled = apply_parity(df_scaled, parity_dict)
    df_scaled['Delta'] = compute_delta(df_scaled, players)

    df_scaled = df_scaled.sort_values(by = 'Delta', key = abs, ascending = False)
    most_relevant_indicators = df_scaled.head(3)

    return df_scaled, most_relevant_indicators

def fetch_coefficients_data(conn, players):

    placeholders = ",".join("?" * len(players))

    query = f"""
    SELECT *
    FROM coefficients
    WHERE "player_name" IN ({placeholders})
    """

    df = pd.read_sql_query(query, conn, params=players)
    return df.drop(['index', 'ranking'], axis = 1).set_index('player_name').T

def main():
    conn = sqlite3.connect('data/db/tennis_abstract_new_version_merged_testing.db')
    cursor = conn.cursor()

    if not table_exists(cursor, "reference_table"):
        raise RuntimeError("❌ reference_table non esiste nel DB.")
    if not table_exists(cursor, "general"):
        raise RuntimeError("❌ general non esiste nel DB.")
    if not table_exists(cursor, "global_averages"):
        raise RuntimeError("❌ global_averages non esiste nel DB.")

    rows = (load_reference_table(cursor))

    categories_label = ['Serve', 'Return', 'Rally', 'Attitude', 'Tactics', 'Efficiency']
    players = [ 'GabrielDiallo', 'LorenzoSonego']
    player1 = players[1]
    player2 = players[0]

    configs = {
        'chart':  create_category_dictionary(rows, categories_label),
        'full':  create_category_dictionary(rows, categories_label, True)
    }

    tables = {}
    for table_type, category_dict in configs.items():
        for label, indicators in category_dict.items():
            tables[(label, table_type)] =  pull_table_values(cursor, indicators, players)
    parity_dict = {row['column_name']: {"parity": row['parity'], "is_ratio": row['is_ratio'] }for row in rows} 
    scaled_tables = {}
    indicators_for_visualization = {}
    for tables_configs in tables.keys():
        if 'chart' in tables_configs:
            label = tables_configs[0]
            scaled_df, best_indicators = delta_col_and_sorting(tables[tables_configs], parity_dict, cursor, players)
            scaled_tables[(label, 'scaled')] = scaled_df
            indicators_for_visualization[label] = best_indicators

    tables.update(scaled_tables)

    tables[('coefficients', 'full')] = fetch_coefficients_data(conn,players)

    export_tables_by_category(tables, rows, f'outputs/excel/tables_{player1}_{player2}.xlsx')

main()