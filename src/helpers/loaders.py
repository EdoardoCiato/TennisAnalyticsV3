import pandas as pd

def load_table(conn, table_name, players=None):
    allowed_tables = {'general', 'min_max_scaled', 'global_averages'}
    if table_name not in allowed_tables:
        raise ValueError(f"Unexpected table name: {table_name}")

    if players:
        placeholders = ','.join('?' for _ in players)
        query = f'SELECT * FROM "{table_name}" WHERE "player_name" IN ({placeholders})'
        df = pd.read_sql_query(query, conn, params=players)
    else:
        query = f'SELECT * FROM "{table_name}"'
        df = pd.read_sql_query(query, conn)

    return df.set_index('player_name')

def load_reference_table(cursor):
    # selecting all the indicators from the reference tables
    cursor.execute('SELECT * FROM reference_table')
    raw_rows = cursor.fetchall()
    rows = []
    # for each indicator, creating a dict with the useful information
    for r in raw_rows:
        rows.append({"indicator": r[0], "parity": r[1], 
                     "column_name": r[2], "topic": r[3],
                       "efficiency": r[4], 'in_chart': r[6], 
                       "reference_group": r[7], "filter_date": r[8],
                       "js": r[9], "is_ratio": r[10]})
    return rows