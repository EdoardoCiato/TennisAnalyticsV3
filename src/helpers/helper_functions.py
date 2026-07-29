import pandas as pd 

def create_category_dictionary(rows, categories, full_table = False):
    # creating a dictionary where the key is the category and the values are the indicators of interest. 
    categories_indicator = {cat: [] for cat in categories}
    # rows is the a list of dictionaries with all the info about the indicators
    for row in rows:
        topic = row['topic']
        column_name = row['column_name']
        efficiency = row['efficiency']
        in_chart = row['in_chart']
        # checking if we are trying to build a full table ( it contains all the indicators)
        if full_table == True:
            in_chart = 1
        # checking if topic in the list of categories because efficiency is not one of those. 
        if topic in categories_indicator and in_chart == 1:
            categories_indicator[topic].append(column_name)
        # check for efficiency. 
        if efficiency == 1 and 'Efficiency' in categories_indicator and in_chart == 1:
            categories_indicator['Efficiency'].append(column_name)

    return categories_indicator

def fetch_coefficients_data(conn, players):

    placeholders = ",".join("?" * len(players))

    query = f"""
    SELECT *
    FROM coefficients
    WHERE "player_name" IN ({placeholders})
    """

    df = pd.read_sql_query(query, conn, params=players)
    return df.drop(['index', 'ranking'], axis = 1).set_index('player_name').T


def table_exists(cur, name):
    # function for sanity check to check that all the needed tables exist. 
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None