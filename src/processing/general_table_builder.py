
from __future__ import annotations
import sqlite3
import re
import statistics
import copy
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.helpers.loaders import load_reference_table
from src.config import DB_PATH

# TODO: rethink table creation. 

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
    rows = load_reference_table(cursor) 
    sql_create_table = f'''
        CREATE TABLE IF NOT EXISTS "general" (
        "ranking" NUMERIC,
        "player_name" TEXT PRIMARY KEY,
        '''
    # rows contains all the information related to the indicators
    indicator_metadata = []
    for r in rows:
        # Clean the indicator name so it can be safely used
        # as a SQL column name.
        column_name = (
            str(r["column_name"])
            .strip()
            .replace('(', '')
            .replace(')', '')
            .replace(',', '')
            .replace("'", '"')
        )
        # we add all the indicators in reference table to the general table we are creating
        sql_create_table += f'"{column_name}" NUMERIC,\n'
        
    sql_create_table += '"matches_analyzed" NUMERIC)'
    cursor.execute(sql_create_table)
    conn.commit()
    return rows

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
    
def shot_direction_JS_data(cursor: sqlite3.Cursor, indicator: str, reference_group: str, player: str)-> tuple[list[tuple], list[str]]:
    """
    Retrieve shot-direction statistics from a Jeff Sackmann charting table.

    Depending on the requested indicator, the function extracts either
    forehand or backhand directional data. 
    To avoid naming conflicts when forehand and backhand statistics are
    later combined, a suffix identifying the shot type is appended to
    each column name.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute SQL queries.
    indicator : str
        Indicator used to determine whether forehand or backhand
        directional statistics should be retrieved.
    reference_group : str
        Name of the database table containing the requested statistics.
    player : str
        Player whose statistics are being retrieved.

    Returns
    -------
    tuple[list[tuple], list[str]]
        A tuple containing:
        - rows: Values retrieved from the database for the selected
          shot-direction statistics.
        - cols: Names of the retrieved columns with a shot-type suffix
          appended (e.g. '_FH' or '_BH').
    """
    if indicator.endswith("_FH"):
        # columns that will be selected 
        cols = ["crosscourt", "down_middle", "down_the_line", "inside_out", "inside_in"]
        # select rows related to forehand 
        row = "F"
        # adding a suffix to name the variables differently 
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
    # adding the suffix because otherwise the columns have the same name and are hard to distinct. 
    cols = list(map(lambda x: x + suffix, cols))
    return rows, cols
    
def data_aggregation_JS(cursor: sqlite3.Cursor, indicator: str, reference_group: str, player: str) -> dict:
    """
    Aggregate directional statistics from Jeff Sackmann charting data.

    The function retrieves the relevant directional statistics for a
    given player, aggregates the values across all available matches,
    and converts the resulting counts into percentage distributions.
    Depending on the reference table, the data is extracted using the
    appropriate helper function for either shot-direction statistics
    or serve-direction / return-depth statistics.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute SQL queries.
    indicator : str
        Indicator used to determine which directional statistics
        should be retrieved.
    reference_group : str
        Name of the database table containing the requested statistics.
    player : str
        Player whose statistics are being aggregated.

    Returns
    -------
    dict
        Dictionary mapping each directional category to its percentage
        share of the total observations. If no observations are
        available, all categories are assigned a value of None.
    """
    # Retrieve the appropriate directional dataset.
    if reference_group == 'mcp_m_stats_shotdirection':
        rows, cols = shot_direction_JS_data(cursor, indicator, reference_group, player)
    else:
        rows, cols = fetch_serve_return_JS_data(cursor, indicator, reference_group, player)
    # Convert the result in a Dataframe
    df = pd.DataFrame(rows, columns=cols)
    #Sum each directional category across all recorded matches.
    totals = df.sum(numeric_only=True)
    # Total number of observations
    side_total = totals.sum()
    # Avoid division by zero when no data is available.
    if side_total == 0:
        return( {col: None for col in cols})
    # Convert counts to percentages.
    return(dict(zip(cols, round(totals/side_total*100,2))))

def serve_speed_conversion(value: float) -> float:
    """
    Convert serve speed from mph to km/h.
    """
    return round(value * CONVERSION_RATE_MPH_KMH,0)

def extract_matches_analyzed(cursor: sqlite3.Cursor, player: str) -> int | None :
    """
    Extract the number of charted matches available for a player.

    The function retrieves the career entry from the reference table
    and extracts the numerical match count from the corresponding
    "Match" field.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute SQL queries.
    player : str
        Player whose match count is being retrieved.

    Returns
    -------
    int | None
        Number of matches available for the player, or None if the
        information cannot be found.
    """
    query = 'SELECT "Match" FROM group_015 WHERE "__player__" = ? AND MATCH LIKE ? '
    cursor.execute(query, (player.strip(),  "%Career%"))
    row = cursor.fetchone()
    if row is None or row[0] is None:
        return None
    # Example : Career (145 matches) --> 145
    match = re.search(r'\d+', str(row[0]))
    # The group function gives the output of the Regex search. 
    return int(match.group()) if match else None

def adjust_range(position: int, i:int) -> tuple[int, int]:
    """
    Compute a ranking range centered around a given position while
    respecting the ranking boundaries.

    The function expands the range by `i` positions above and below the
    target ranking. If the resulting range exceeds the available ranking
    limits (0-200), the missing positions are compensated for on the
    opposite side to preserve the intended range size.

    Parameters
    ----------
    position : int
        Ranking position of the target player.
    i : int
        Number of positions to include above and below the target
        ranking.

    Returns
    -------
    tuple[int, int]
        Lower and upper bounds of the adjusted ranking range.
    """
    # Players at the extremes of the ranking may not have enough players
    # above or below them. The range is expanded on the opposite side to
    # maintain the desired sample size. 
    adjustment = 0

    final_spot = position + i
    initial_spot = position - i
    # if the final spot is above 200,
    # we top the upper boundary to 200 and add the difference to the initial spot
    if final_spot > 200:
        adjustment = final_spot - 200
        final_spot = 200
        initial_spot -= adjustment
    # if the initial spot is below 0,
    # we top the low boundary to 0 and add the difference to the final spot
    if initial_spot < 0:
        adjustment = -initial_spot
        initial_spot = 0
        final_spot += adjustment
    
    return initial_spot, final_spot

def pull_range_players(position: int, i: int, players: list, indicator: str, raw_player_table: dict) -> list:
        """
    Extract indicator values from players ranked near a target position.

    The function identifies all players within a ranking range centered
    around the specified position, retrieves the requested indicator for
    each player, and returns the available values after excluding
    missing observations.
    The ranking range is adjusted when the target player is near the
    top or bottom of the rankings to preserve the intended sample size.

    Parameters
    ----------
    position : int
        Ranking position of the target player.
    i : int
        Number of ranking positions to include above and below the
        target player.
    players : list
        Ordered list of player names, where the list position
        corresponds to the ranking position.
    indicator : str
        Indicator whose values should be extracted.
    raw_player_table : dict
        Dictionary containing all player statistics.

    Returns
    -------
    list
        Valid values of the requested indicator for players within the
        adjusted ranking range.
    """
        # Ensuring the validity of the range
        values = []
        # obtain the corrected range
        initial_spot, final_spot = adjust_range(position, i)
        local_range_players = []
        for i, row in enumerate(players):
            # From numerical range, to actual players. 
            if i >= initial_spot and i <= final_spot:
                local_range_players.append(row.strip())

        for pl in local_range_players:
            row = raw_player_table.get(pl)
            if row:
                value = row.get(indicator)
                # Include values only if not missing. 
                if value not in [None, 'NA', '-']:
                    values.append(value)

        return values

def handling_NA( player: str, indicator: str, players:list, raw_player_table: dict) -> float:
    """
    Impute a missing indicator value using data from similarly ranked players.

    The function progressively expands the ranking window around the
    target player and collects valid values for the requested indicator.
    Once a sufficiently large sample is available, the missing value is
    replaced with the median of the collected observations.

    Parameters
    ----------
    player : str
        Player whose missing value is being imputed.
    indicator : str
        Indicator requiring imputation.
    players : list
        Ordered list of player names, where the list position
        corresponds to the ranking position.
    raw_player_table : dict
        Dictionary containing all player statistics.

    Returns
    -------
    float
        Imputed value for the requested indicator.
    """
    imputed_val = 0
    # Create a dictionary, player = key, ranking = value.
    position_map = {player: i for i, player in enumerate(players)}
    # Extract the position of the target player. 
    position = position_map[player]
    for i in range(20,61,10):
        values = pull_range_players(position, i, players, indicator, raw_player_table)
        # Check if there are enough players in the range.
        # Require at least 30 observations to obtain a reasonably stable estimate.
        # If the condition is not met, expand the window (i).
        if len(values) >= 30:
            # Calculate and return the median.
            imputed_val = round(statistics.median(values), 1)
            break

    return imputed_val

def main():
    # Connect to the db. 
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Drop the already existing table. 
    cursor.execute('''
                DROP TABLE IF EXISTS general
                ''')
    # Create the new table with the columns from the reference table and 
    # retrieve the metadata for each indicator. 
    indicator_metadata = create_general_table(cursor, conn)
    column_names = [indicator['column_name'] for indicator in indicator_metadata]
    column_names.append('matches_analyzed')
    column_names.insert(0, 'ranking')
    # Load players in ranking order.
    players = []
    with open("data/raw/top200.txt") as file:
            for line in file:
                players.append(line.strip())
    missing_data = {}
    missing_info = []
    raw_player_table = {}
    ranking = 1
    for player in players:
        missing_info = []
        player = player.strip()
        # Build one row per player, using indicator names as keys.
        player_data = { "ranking": ranking, "player_name": player}
        for r in indicator_metadata:
            indicator = r['indicator']
            # Skip the process if the indicator is already included in the dictionary
            if indicator in player_data:
                continue
            filter_date = r['filter_date']
            reference_group = r['reference_group']
            # If the indicator is a JeffSackmann indicator, convert the name and calculate it with the appropriate function. 
            if r["js"] == 1:
                name = convert_ta_name_to_js(player)
                value_dict_js = data_aggregation_JS(cursor, indicator, reference_group, name)
                has_no_na = all(value is not None for value in value_dict_js.values())
                if has_no_na:
                    player_data.update(value_dict_js)
                else:
                    missing_info.extend([key for key, value in value_dict_js.items() if value is None])
            else:
                value = fetch_indicator_value(indicator, player, reference_group, filter_date, cursor)
                if value in [None, "NA", "-"]:
                    missing_info.append(indicator)
                # Convert the speed when dealing with a serve speed indicator. 
                elif indicator in ["1st_Avg" , "1st_T_Avg", "1st_Wide_Avg", "2nd_Avg", "2nd_T_Avg", "2nd_Wide_Avg"]:
                    value = serve_speed_conversion(value)
                # Add the element to the row data dictionary
                player_data[indicator] = value

            if missing_info:
                # Add the missing info to the NA dictionary.
                missing_data.setdefault(player, []).extend(missing_info)
        ranking += 1
        # Extract matches analyzed.
        player_data['matches_analyzed'] = extract_matches_analyzed(cursor, player)
        # Once all the information for the player is available, add it to the raw table. 
        raw_player_table[player] = player_data
    # creating a copy to leave the original untouched

    imputed_player_table = copy.deepcopy(raw_player_table)
    # Imputed flags stores what player-indicator combo have been imputed for future reference. 
    imputed_flags = {}
    # For loop to impute the missing values. 
    for pl, indicators in missing_data.items():
        for ind in indicators:
            imputed_player_table[pl][ind] = handling_NA( pl, ind, players, raw_player_table)
            imputed_flags[(pl, ind)] = 1
    # The imputed table is the final version inserted into the database.
    general_df = pd.DataFrame.from_dict(imputed_player_table, orient='index').set_index('player_name')
    general_df.columns = column_names
    general_df.to_sql(name = 'general', con=conn, if_exists = 'replace')
    # Commit all the changes to the DB and close the connection
    conn.commit()
    conn.close()

main()
 