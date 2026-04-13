import pandas as pd
import sqlite3

def load_reference_table(cursor):
    cursor.execute('SELECT * FROM reference_table')
    raw_rows = cursor.fetchall()
    rows = []
    for r in raw_rows:
        rows.append({"indicator": r[0], "column_name": r[2], "topic": r[3], "efficiency": r[4], 'in_chart': r[6]})
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
    df = df.transpose()
    return df

    # check if it makes more sense to upload the table directly here or if i should do it in another function

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
    print(tables)
main()