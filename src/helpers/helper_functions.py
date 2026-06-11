def create_category_dictionary(rows, categories, full_table = False):
    # creating a dictionary where the key is the category and the values are the indicators of interest. 
    categories_indicator = {cat: ['player_name'] for cat in categories}
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

def load_reference_table(cursor):
    # selecting all the indicators from the reference tables
    cursor.execute('SELECT * FROM reference_table')
    raw_rows = cursor.fetchall()
    rows = []
    # for each indicator, creating a dict with the useful information
    for r in raw_rows:
        rows.append({"indicator": r[0], "parity": r[1], "column_name": r[2], "topic": r[3], "efficiency": r[4], 'in_chart': r[6], "is_ratio": r[10]})
    return rows