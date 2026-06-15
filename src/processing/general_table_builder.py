
from __future__ import annotations

import sqlite3
import re
import statistics
import copy
import pandas as pd

# TODO: transform speed from mp/h to km/h 

def create_table(cursor, conn):
    # get info on indicators from reference table
    cursor.execute('SELECT * from "reference_table"')
    raw_rows = cursor.fetchall()
    code_create_table = f'''
        CREATE TABLE IF NOT EXISTS "general" (
        "ranking" NUMERIC,
        "player_name" TEXT PRIMARY KEY,
        '''
    rows = []
    for r in raw_rows:
        column_name = (
            str(r[2])
            .strip()
            .replace('(', '')
            .replace(')', '')
            .replace(',', '')
            .replace("'", '"')
        )
        code_create_table += f'"{column_name}" NUMERIC,\n'
        rows.append({"indicator": r[0], "column_name": r[2], "reference_group": r[7], "filter_date": r[8],"js": r[9]})
    code_create_table += '"matches_analyzed" NUMERIC)'
    cursor.execute(code_create_table)
    conn.commit()
    return rows

def extract_indicator(indicator, player, reference_group, filter_date, cursor):   
        query = f'''
        SELECT "{indicator.strip()}" FROM "{reference_group}"
        WHERE "__player__" = ? 
        AND "{filter_date}" LIKE ?
        ORDER BY "M" DESC
        LIMIT 1
                    '''
        cursor.execute(query, (player.strip(),  "%Career%"))
        row = cursor.fetchone()
        value = row[0] if row is not None else "NA"

        if value not in [None, 'NA', '-']:
            if isinstance(value, str) and '%' in value:
                value = float(value.replace('%', ''))
            else:
                value = float(value)

        return value
    
def name_handling_JS(player):
    # tennis abstract name: CarlosAlcaraz
    # JS name: Carlos Alcaraz
    player_list = re.findall('[A-Z][^A-Z]*', player)
    js_name = " ".join(player_list)
    return js_name

def serve_return_JS_data(cursor, indicator, reference_group, player ):

    if indicator in ["deuce_wide", "deuce_middle", "deuce_t"]:
        cols = ["deuce_wide", "deuce_middle", "deuce_t"]
    
    elif indicator.strip() in ["shallow", "deep", "very_deep"]:
        cols = ["shallow", "deep", "very_deep"]

    elif indicator in["ad_wide", "ad_middle", "ad_t"]:
        cols = ["ad_wide", "ad_middle", "ad_t"]
    
    query = f'''
                SELECT {", ".join(f'"{c}"' for c in cols)}
                FROM "{reference_group}"
                WHERE "player" = ? and "row" = "Total"
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
        rows, cols = serve_return_JS_data(cursor, indicator, reference_group, player)
    df = pd.DataFrame(rows, columns=cols)
    #columns-wise sum --> sum of an indicator over all the matches recorded
    totals = df.sum(numeric_only=True)
    # row-wise sum --> total number of forehands or backhands
    side_total = totals.sum()
    if side_total == 0:
        return( {col: None for col in cols})
    


    return(dict(zip(cols, round(totals/side_total*100,2))))


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

    rows = create_table(cursor, conn)
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
                name = name_handling_JS(player)
                value = data_aggregation_JS(cursor, indicator, reference_group, name)
                if value is None:
                    missing_info.append(indicator)
                else:
                    row_data.update(value)   # value is a dict
            else:
                value = extract_indicator(indicator, player, reference_group, filter_date, cursor)

                if value in [None, "NA", "-"]:
                    missing_info.append(indicator)

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
 