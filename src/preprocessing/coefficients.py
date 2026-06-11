import sqlite3
import numpy as np
import pandas as pd
from src.helpers.helper_functions import create_category_dictionary
from src.helpers.helper_functions import load_reference_table

DB_PATH = "data/db/tennis_abstract_new_version_merged_testing.db"

def pull_columns_values(conn):

    df = pd.read_sql_query('SELECT * FROM general', conn)
    return df

def main ():
    conn = sqlite3.connect("data/db/tennis_abstract_new_version_merged_testing.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    columns = []
    cursor.execute("PRAGMA table_info(general)")
    for row in cursor.fetchall():
        columns.append(row["name"])
    full_df = pull_columns_values(conn)
    # print perricard rally because the values are too high
    matches_analyzed = full_df['matches_analyzed']
    variables_df = full_df.copy().drop(['ranking', 'player_name', 'matches_analyzed'], axis = 1)
    percentiles_df = variables_df.rank(pct=True) * 100
    percentiles_df = percentiles_df.apply ( lambda x: round(x, 3))
    rows = load_reference_table(cursor)
    categories_label = ['Serve', 'Return', 'Rally', 'Attitude', 'Tactics', 'Efficiency']
    dict_categories = create_category_dictionary(rows, categories_label)

    coefficients_df = full_df[['ranking', 'player_name']]
    for label, indicators in dict_categories.items():
        indicators = indicators[1:]
        coefficients_df[label] = percentiles_df[indicators].mean(axis=1)
    
    print(coefficients_df)






main()

