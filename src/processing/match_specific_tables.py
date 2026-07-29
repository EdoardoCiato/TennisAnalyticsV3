import pandas as pd
import sqlite3
import sys
from pathlib import Path
import json

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.export.excel_exporter import export_tables_by_category
from src.helpers.helper_functions import create_category_dictionary, fetch_coefficients_data, table_exists
from src.helpers.loaders import load_reference_table, load_table

PLAYERS = [ 'TaylorFritz', 'LorenzoSonego']
DB_PATH = 'data/db/tennis_abstract_new_version_merged_testing.db'
CATEGORIES_LABELS = ['Serve', 'Return', 'Rally', 'Attitude', 'Tactics', 'Efficiency']

# TODO: in pull table values, i hard-cded "player_name" see if there are other soluitons. 
#       I did it because indicators are just the variables we want to fetch. 

# TODO: check docstring for delta col and sorting

def compute_delta(df: pd.DataFrame) -> pd.DataFrame:
    ''' Calculates the delta between scaled indicators to find the most relevant indicators'''
    p1 = df[PLAYERS[0]].copy()
    p2 = df[PLAYERS[1]].copy()
    return p2 - p1

def pull_table_values(df: pd.DataFrame, indicators: list, avg_df: pd.DataFrame) -> pd.DataFrame:
    ''' Retrieves the values for a set of indicators for specific players.
        After fetching the values, the df is transposed and for visual purposes.    
    '''
    if not indicators:
        return pd.DataFrame(columns=["Player"])
    if not PLAYERS:
        return pd.Dataframe(columns=indicators)
    players_df = df[indicators]
    avg_row =  avg_df[indicators]
    category_df = pd.concat([avg_row, players_df], axis=0).T
    category_df.index.name = 'indicator'
    category_df.columns.name = 'player'
    return category_df

def delta_col_and_sorting(df: pd.DataFrame) -> tuple[(pd.DataFrame, list)]:
    ''' Calculates the delta between the data of the two players to find the most important 
        indicators. 
        First create the "delta" column and then use it to sort the values.
        N.B: the difference is calculated as Sonego - the other player
    '''
    df_scaled = df.copy()
    df_scaled['Delta'] = compute_delta(df_scaled)

    df_scaled = df_scaled.sort_values(by = 'Delta', key = abs, ascending = False)
    most_relevant_indicators = df_scaled.head(3)

    # TODO: save values in a json file and read them from there

    return df_scaled, most_relevant_indicators

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if not table_exists(cursor, "reference_table"):
        raise RuntimeError("reference_table does not exist in the DB.")
    if not table_exists(cursor, "general"):
        raise RuntimeError("general does not exist in the DB.")
    if not table_exists(cursor, "global_averages"):
        raise RuntimeError("global_averagesdoes not exist in the DB.")
    if not table_exists(cursor, 'min_max_scaled'):
        raise RuntimeError('min_max_scaled does not exist in the DB.')

    rows = load_reference_table(cursor)

    player1 = PLAYERS[1]
    player2 = PLAYERS[0]
    # Load the dfs.
    general_df = load_table(conn, 'general', PLAYERS)
    scaled_df = load_table(conn, 'min_max_scaled', PLAYERS)
    avg_df = load_table(conn, 'global_averages')
    # Create different configuration for the tables to build in excel. 
    configs = {
        'chart':  create_category_dictionary(rows=rows, categories=CATEGORIES_LABELS),
        'full':  create_category_dictionary(rows = rows, categories=CATEGORIES_LABELS, full_table= True),
        'scaled': create_category_dictionary(rows = rows, categories=CATEGORIES_LABELS,full_table= True)
    }

    tables = {}
    # Iterate over each table type and the associated dictionary containing the category name and the respective indicators. 
    for table_type, category_dict in configs.items():
        for label, indicators in category_dict.items():
            # For the scaled table, use the scaled df with min-max. 
            if table_type == 'scaled': 
                df = scaled_df
            else:
                df = general_df     
            # For each label and table type pull the df with the desired data.
            tables[(label, table_type)] =  pull_table_values(df=df, indicators=indicators, avg_df = avg_df)
    indicators_for_visualization = {}
    for tables_configs in tables.keys():
        # For each label of scaled tables, sort them out by absolute value and pull the 3 main indicators.  
        if 'scaled' in tables_configs:
            label = tables_configs[0]
            scaled_df, best_indicators = delta_col_and_sorting(df = tables[tables_configs])
            # Remove the ATP average because it's a comparison with the scaled data of the two players. 
            tables[tables_configs] = scaled_df.drop(['ATP average'], axis = 1)
            indicators_for_visualization[label] = best_indicators

    # Add the coefficients table to the excel. 
    tables[('coefficients', 'full')] = fetch_coefficients_data(conn=conn, players=PLAYERS)

    #Export each table to excel. 
    export_tables_by_category(tables=tables, rows=rows, output_file=f'outputs/excel/tables_{player1}_{player2}.xlsx')

if __name__ == '__main__':
    main()