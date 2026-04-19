import pandas as pd
import sqlite3
from adjusted_metrics import min_max_scaling, compute_delta, apply_parity

def load_reference_table(cursor):
    cursor.execute('SELECT * FROM reference_table')
    raw_rows = cursor.fetchall()
    rows = []
    for r in raw_rows:
        rows.append({"indicator": r[0], "parity": r[1], "column_name": r[2], "topic": r[3], "efficiency": r[4], 'in_chart': r[6], "is_ratio": r[10]})
    return rows

def table_exists(cur, name):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def create_category_dictionary(rows, categories, full_table = False):
    categories_indicator = {cat: ['player_name'] for cat in categories}
    for row in rows:
        topic = row['topic']
        column_name = row['column_name']
        efficiency = row['efficiency']
        in_chart = row['in_chart']
        if full_table == True:
            in_chart = 1
        if topic in categories_indicator and in_chart == 1:
            categories_indicator[topic].append(column_name)
        if efficiency == 1 and 'Efficiency' in categories_indicator and in_chart == 1:
            categories_indicator['Efficiency'].append(column_name)

    return categories_indicator

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
    df_scaled = min_max_scaling(cursor, players)
    df_scaled = apply_parity(df_scaled, parity_dict)
    df_scaled['Delta'] = compute_delta(df_scaled, players)

    df_scaled = df_scaled.sort_values(by = 'Delta', key = abs, ascending = False)

    return df_scaled

def pull_top3_indicators_for_report():
    pass

def main():
    conn = sqlite3.connect('tennis_abstract_new_version_merged_testing.db')
    cursor = conn.cursor()

    if not table_exists(cursor, "reference_table"):
        raise RuntimeError("❌ reference_table non esiste nel DB.")
    if not table_exists(cursor, "general"):
        raise RuntimeError("❌ general non esiste nel DB.")
    if not table_exists(cursor, "global_averages"):
        raise RuntimeError("❌ global_averages non esiste nel DB.")

    rows = (load_reference_table(cursor))

    categories_label = ['Serve', 'Return', 'Rally', 'Attitude', 'Tactics', 'Efficiency']
    players = ['CarlosAlcaraz', 'JannikSinner']

    configs = {
        'chart':  create_category_dictionary(rows, categories_label),
        'full':  create_category_dictionary(rows, categories_label, True)
    }

    tables = {}
    for table_type, category_dict in configs.items():
        for label, indicators in category_dict.items():
            tables[(label, table_type)] =  pull_table_values(cursor, indicators, players)
    parity_dict = {row['column_name']: {"parity": row['parity'], "is_ratio": row['is_ratio'] }for row in rows} 
    for dfs in tables.values():
        print(delta_col_and_sorting(dfs, parity_dict, cursor, players))

    

main()