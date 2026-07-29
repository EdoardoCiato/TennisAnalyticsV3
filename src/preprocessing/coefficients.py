from __future__ import annotations
import sqlite3
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.manifold import TSNE
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.helpers.helper_functions import create_category_dictionary
from src.helpers.helper_functions import load_reference_table

DB_PATH = "data/db/tennis_abstract_new_version_merged_testing.db"

# TODO: upload table to sql. 
# TODO: Correlation matrix --> pick only relevant variables. 

# def correlation_analysis(df, indicators, title):
#     corr = df[indicators].corr()
#     print(corr)
#     upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
#     to_drop = [column for column in upper.columns if any(upper[column] > 0.80)]

#     corr.drop(to_drop, axis=1, inplace=True)
#     indicators = corr.columns

#     #

#     fig, ax = plt.subplots(figsize=(len(indicators) * 1.2, len(indicators)))
    
#     sns.heatmap(
#         corr,
#         annot=True,
#         cmap='coolwarm',
#         fmt='.2f',
#         vmin=-1, vmax=1,          # fix the color scale
#         linewidths=0.5,           # grid lines for readability
#         ax=ax
#     )
    
#     ax.set_title(title, fontsize=14, pad=12)
#     plt.xticks(rotation=45, ha='right')
#     plt.tight_layout()
#     plt.show()


def fetch_general_table_data(conn):

    df = pd.read_sql_query('SELECT * FROM general', conn)
    return df


def main ():
    conn = sqlite3.connect("data/db/tennis_abstract_new_version_merged_testing.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    # Obtain the name of the columns in general. 
    cursor.execute("PRAGMA table_info(general)")
    full_df = fetch_general_table_data(conn)
    # Df containing only variables to be analyzed.
    variables_df = full_df.copy().drop(['ranking', 'player_name', 'matches_analyzed'], axis = 1)
    # Load reference table information to retrieve indicator metadata.
    rows = load_reference_table(cursor)
    # List containing only the indicator with negative parity ( the lower the better).
    negative_parity_cols = [r['column_name'] for r in rows if  r['parity'] == -1 ]
    # Find the positive parity columns by exclusion. 
    positive_parity_cols = list(set(variables_df.columns)- set(negative_parity_cols))
    # Calculate percentile for positive variables. 
    variables_df[positive_parity_cols] = variables_df[positive_parity_cols].rank(pct=True) * 100
    # Calculate percentile for negative variables, the lower the better.
    variables_df[negative_parity_cols] = variables_df[negative_parity_cols].rank(pct=True, ascending= False) * 100
    # Round for visual purposes.
    percentiles_df = variables_df.copy().apply( lambda x: round(x, 3)) 
    categories_label = ['Serve', 'Return', 'Rally', 'Attitude', 'Efficiency']
    # Create a dictionary key: category, value list of indicators belonging to that category. 
    dict_categories = create_category_dictionary(rows, categories_label, True)
    # Create the final df. 
    coefficients_df = full_df.copy()[['ranking', 'player_name']]
    min_max_scaling(cursor,)
    for label, indicators in dict_categories.items():
        # Start from 1 because each group contains player_name
        indicators.remove('player_name')
        print(indicators)
        # Index = average of every quantile for that specific category.) 
        coefficients_df[label] = percentiles_df[indicators].mean(axis=1)

    coefficients_df['global'] = coefficients_df[categories_label].mean(axis=1)
    # print(coefficients_df[coefficients_df['player_name'] == "GabrielDiallo"])
    # print(coefficients_df.sort_values('global', ascending=False))

        
    coefficients_df.to_sql(name='coefficients', con=conn, if_exists='replace')
    
    conn.close()

main()
