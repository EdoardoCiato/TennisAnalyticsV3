
from __future__ import annotations
import sqlite3
import re
import statistics
import copy
import pandas as pd

CONVERSION_RATE_MPH_KMH = 1.60934

INDICATOR_TO_COLS = {
    "deuce_wide":   ["deuce_wide", "deuce_middle", "deuce_t"],
    "deuce_middle": ["deuce_wide", "deuce_middle", "deuce_t"],
    "deuce_t":      ["deuce_wide", "deuce_middle", "deuce_t"],
    "ad_wide":      ["ad_wide", "ad_middle", "ad_t"],
    "ad_middle":    ["ad_wide", "ad_middle", "ad_t"],
    "ad_t":         ["ad_wide", "ad_middle", "ad_t"],
    "shallow":      ["shallow", "deep", "very_deep"],
    "deep":         ["shallow", "deep", "very_deep"],
    "very_deep":    ["shallow", "deep", "very_deep"],
}

def create_general_table(cursor: sqlite3.Cursor, conn: sqlite3.Connection) -> list[dict]:

    """
    Create the general summary table and return the indicator metadata.

    The function reads all indicators from the reference table,
    dynamically builds the SQL schema of the general table,
    creates the table if it does not already exist, and returns
    a list containing the metadata required for later processing.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute SQL queries.
    conn : sqlite3.Connection
        Active SQLite database connection.

    Returns
    -------
    list[dict]
        Metadata for each indicator, including:
        - indicator --> name of the column in the DB; 
        - column_name --> expanded name of the column ( for clarity purposes); 
        - reference_group --> table of DB where the data can be found; 
        - filter_date --> keyword use to filter rows to obtain career data; 
        - js --> whether the data was obtained from JeffSackmann repo; 
    """

     # Retrieve all indicator definitions from the reference table.
    cursor.execute('SELECT * from "reference_table"')
    raw_rows = cursor.fetchall()
    sql_create_table = f'''
        CREATE TABLE IF NOT EXISTS "general" (
        "ranking" NUMERIC,
        "player_name" TEXT PRIMARY KEY,
        '''
    # rows contains all the information related to the indicators
    indicator_metadata = []
    for r in raw_rows:
        # Clean the indicator name so it can be safely used
        # as a SQL column name.
        column_name = (
            str(r[2])
            .strip()
            .replace('(', '')
            .replace(')', '')
            .replace(',', '')
            .replace("'", '"')
        )
        # we add all the indicators in reference table to the general table we are creating
        sql_create_table += f'"{column_name}" NUMERIC,\n'

        indicator_metadata.append({ "indicator": r[0],
                                    "column_name": r[2],
                                    "reference_group": r[7],
                                    "filter_date": r[8],
                                    "js": r[9]})
        
    sql_create_table += '"matches_analyzed" NUMERIC)'
    cursor.execute(sql_create_table)
    conn.commit()
    return indicator_metadata

def fetch_indicator_value(indicator: str, player: str, reference_group: str, filter_date: str, cursor: sqlite3.Cursor) -> float | str :  
        """
    Extract the value of a specific indicator for a given player.

    The function queries the appropriate reference table, retrieves the
    most relevant row based on sample size, and converts percentage values
    to floats when needed. If no valid value is found, it returns "NA".

    Parameters
    ----------
    indicator : str
        Name of the indicator column to retrieve.
    player : str
        Player name used to filter the query.
    reference_group : str
        Table containing the indicator values.
    filter_date : str
        Column used to filter career data.
    cursor : sqlite3.Cursor
        SQLite cursor used to execute the query.

    Returns
    -------
    float | str
        Numerical value for the indicator, or "NA" if unavailable.
    """

        # Order by match because some low ranking players may have more challengers matches than ATP
        # matches and we pick the one with the highest sample size. 
        query = f'''
        SELECT "{indicator.strip()}" FROM "{reference_group}"
        WHERE "__player__" = ? 
        AND "{filter_date}" LIKE ?
        ORDER BY "M" DESC
        LIMIT 1
                    '''
        cursor.execute(query, (player.strip(),  "%Career%"))
        row = cursor.fetchone()
        if row is None or row[0] in (None, "NA", "-"):
            return "NA"
        value = row[0]

        if isinstance(value, str):
            value = value.replace("%", "")
        value = float(value)

        return value
    
def convert_ta_name_to_js(player: str) -> str:
    """
    Convert a Tennis Abstract player name into the format used by
    the Jeff Sackmann datasets.

    Tennis Abstract stores player names without spaces (e.g. "CarlosAlcaraz"),
    while Jeff Sackmann datasets use a space-separated format
    (e.g. "Carlos Alcaraz"). The function splits the name at each
    capital letter and joins the resulting parts with spaces.

    Parameters
    ----------
    player : str
        Player name in Tennis Abstract format.

    Returns
    -------
    str
        Player name formatted according to the Jeff Sackmann convention.

    Examples
    --------
    >>> convert_ta_name_to_js("CarlosAlcaraz")
    'Carlos Alcaraz'
    """
    player_list = re.findall('[A-Z][^A-Z]*', player)
    js_name = " ".join(player_list)
    return js_name

def fetch_serve_return_JS_data(cursor: sqlite3.Cursor, indicator: str, reference_group: str, player:str ) -> tuple[list[tuple], list[str]]:
    """
    Retrieve serve-direction or return-depth data from a Jeff Sackmann table.

    Depending on the requested indicator, the function identifies the
    corresponding indicator group and retrieves all related columns from
    the specified table. This function is used exclusively for serve
    direction and return depth statistics, which require grouped
    extraction rather than single-indicator retrieval (fetch_indicator_value)

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute SQL queries.
    indicator : str
        Indicator used to determine which group of statistics should
        be extracted.
    reference_group : str
        Name of the database table containing the requested statistics.
    player : str
        Player whose statistics are being retrieved.

    Returns
    -------
    tuple[list[tuple], list[str]]
        A tuple containing:
        - rows: Query result returned by SQLite.
        - cols: Names of the retrieved columns.
    """

    cols = INDICATOR_TO_COLS.get(indicator)
    if cols is None:
        raise ValueError(f"Unknown indicator: {indicator}")
    
    query = f'''
                SELECT {", ".join(f'"{c}"' for c in cols)}
                FROM "{reference_group}"
                WHERE "player" = ? 
                AND "row" = "Total"
                    ''' 
    cursor.execute(query, (player,))
    rows = cursor.fetchall()

    return rows, cols
    
def shot_direction_JS_data(cursor, indicator, reference_group, player):
    if "FH" in indicator:
        cols = ["crosscourt", "down_middle", "down_the_line", "inside_out", "inside_in"]
        row = "B"
        suffix = "_FH"
    else:
        cols = ["crosscourt", "down_middle", "down_the_line"]
        row = "B"
        suffix = "_BH"
    
    query = f'''
                SELECT {", ".join(f'"{c}"' for c in cols)} FROM "{reference_group}"
                WHERE "player" = ? and "row" = ?
                    ''' 
    cursor.execute(query, (player, row))
    
    rows = cursor.fetchall()
    cols = list(map(lambda x: x + suffix, cols))
    return rows, cols
    
def data_aggregation_JS(cursor, indicator, reference_group, player):

    if reference_group == 'mcp_m_stats_shotdirection':
        rows, cols = shot_direction_JS_data(cursor, indicator, reference_group, player)
    else:
        rows, cols = fetch_serve_return_JS_data(cursor, indicator, reference_group, player)
    df = pd.DataFrame(rows, columns=cols)
    #columns-wise sum --> sum of an indicator over all the matches recorded
    totals = df.sum(numeric_only=True)
    # row-wise sum --> total number of forehands or backhands
    side_total = totals.sum()
    if side_total == 0:
        return( {col: None for col in cols})
    
    return(dict(zip(cols, round(totals/side_total*100,2))))


def serve_speed_conversion(value):
    return round(value * CONVERSION_RATE_MPH_KMH,0)

def extract_games_analyzed(cursor, player):
    query = 'SELECT "Match" FROM group_015 WHERE "__player__" = ? AND MATCH LIKE ? '
    cursor.execute(query, (player.strip(),  "%Career%"))
    row = cursor.fetchone()
    if row is None or row[0] is None:
        return None
    match = re.search(r'\d+', str(row[0]))
    return int(match.group()) if match else None

def insert_row_into_general_table(row_data, cursor):
    values = list(row_data.values())
    placeholders = ",".join(['?']*len(values))
    query = f'''
        INSERT INTO general VALUES ({placeholders})
    '''
    cursor.execute(query, values)

def check_range(position, i):
    # Players at the extremes of the ranking may not have enough players above or below,
    # we adjust by adding players to the opposite side to compensate
    adjustment = 0
    final_spot = position + i
    initial_spot = position - i
    if final_spot > 200:
        adjustment = final_spot - 200
        final_spot = 200
        initial_spot -= adjustment
    if initial_spot < 0:
        adjustment = -initial_spot
        initial_spot = 0
        final_spot += adjustment
    
    return initial_spot, final_spot


def pull_range_players(position, i, players, indicator, raw_table):
        # Ensuring the validity of the range
        percentage = None
        values = []
        # obtain the corrected range
        initial_spot, final_spot = check_range(position, i)
        local_range_players = []
        for i, row in enumerate(players):
            # Going from numerical range, to actual players. 
            if i >= initial_spot and i <= final_spot:
                local_range_players.append(row.strip())

        for pl in local_range_players:
            row = raw_table.get(pl)
            if row:
                value = row.get(indicator)

                if value not in [None, 'NA', '-']:
                    values.append(value)

        return values, percentage
def handling_NA( player, indicator, players, raw_table):
    # getting the ranking of a player
    imputed_val = 0
    position_map = {player: i for i, player in enumerate(players)}
    position = position_map[player]
    for i in range(20,61,10):
        values, percentage = pull_range_players(position, i, players, indicator, raw_table)
        # Checking if there are enough players in the range
        if len(values) >= 30:
            # calculate and return the median
            imputed_val =  (round(statistics.median(values), 1))
            # if percentage:
            #     imputed_val = str(imputed_val)+'%'
            break

    return imputed_val

def main ():
    conn = sqlite3.connect("data/db/tennis_abstract_new_version_merged_testing.db")
    cursor = conn.cursor()
    cursor.execute('''
                DROP TABLE IF EXISTS general
                ''')
    rows = create_general_table(cursor, conn)
    print(type(rows))
    players = []
    with open("data/raw/top200.txt") as file:
            for line in file:
                players.append(line.strip())
    PLAYERS  = players
    missing_data = {}
    missing_info = []
    raw_table = {}
    i = 1
    for player in PLAYERS:
        player = player.strip()
        row_data = { "ranking": i, "player_name": player}
        # rows contains info about indicators, so for each player we get the info for that 
        # indicator and find the associated value
        for r in rows:
            indicator = r['indicator']
            if indicator in row_data.keys(): continue
            filter_date = r['filter_date']
            reference_group = r['reference_group']
            if r["js"] == 1:
                name = convert_ta_name_to_js(player)
                value = data_aggregation_JS(cursor, indicator, reference_group, name)
                if value is None:
                    missing_info.append(indicator)
                else:
                    row_data.update(value)   # value is a dict
            else:
                value = fetch_indicator_value(indicator, player, reference_group, filter_date, cursor)
                if value in [None, "NA", "-"]:
                    missing_info.append(indicator)
                elif indicator in ["1st_Avg" , "1st_T_Avg", "1st_Wide_Avg", "2nd_Avg", "2nd_T_Avg", "2nd_Wide_Avg"]:
                    value = serve_speed_conversion(value)

                row_data[indicator] = value

            if missing_info:
                missing_data.setdefault(player, []).extend(missing_info)
            missing_info = []
        i += 1
        row_data['matches_analyzed'] = extract_games_analyzed(cursor, player)
        raw_table[player] = row_data
    # creating a copy to leave the original untouched
    imputed_table = copy.deepcopy(raw_table)
    imputed_flags = {}
    for pl, indicators in missing_data.items():
        for ind in indicators:
            imputed_table[pl][ind] = (handling_NA( pl, ind, PLAYERS, raw_table))
            imputed_flags[(pl, ind)] = 1
    for row in imputed_table.values():
        insert_row_into_general_table(row, cursor)
            
    conn.commit()
    conn.close()

main()
 