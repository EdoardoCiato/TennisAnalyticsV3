import sqlite3
import re
import statistics
import copy

def create_table(cursor, conn):
    # get info on indicators from reference table
    cursor.execute('SELECT * from "reference_table"')
    raw_rows = cursor.fetchall()
    code_create_table = f'''
        CREATE TABLE IF NOT EXISTS "general" (
        "player_name" TEXT PRIMARY KEY,
        '''
    rows = []
    for r in raw_rows:
        code_create_table += '"' + f'{str(r[2]).strip().replace('(','').replace(')','').replace(',','').replace("'", '"')}' + '"'+ ' NUMERIC ,\n '  
        rows.append({"indicator": r[0], "column_name": r[2], "reference_group": r[7], "filter_date": r[8],"js": r[9]})
    code_create_table = code_create_table[:-3]
    code_create_table += ")"
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

def data_aggregation_JS(indicator, player, reference_group, cursor):
    if reference_group == "mcp_m_stats_returndepth":
        # Returndepth values are absolute values, not a percentage
        query = f'''
            SELECT "{indicator}", "returnable" FROM "{reference_group}"
            WHERE "player" = ?
                '''
    else:
        query = f'''
                    SELECT "{indicator}" FROM "{reference_group}"
                    WHERE "player" = ?
                        '''
    cursor.execute(query, (player,))
    rows = cursor.fetchall()
    if rows == []: 
        return None
    num = 0
    den = 0
    # No career values, so we calculate the average over the career
    for pair in rows:
        num += pair[0]
        try :
            den += pair[1]
        except IndexError:
            den = 0
    if len(rows[0]) > 1:
        value = round((num / den) * 100, 1)
    else:
        value = round(num/(len(rows)), 1)

    return (value)

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
    conn = sqlite3.connect("tennis_abstract_new_version_merged_testing.db")
    cursor = conn.cursor()

    cursor.execute('''
                DROP TABLE IF EXISTS general
                ''')

    rows = create_table(cursor, conn)
    players = []
    with open("top200.txt") as file:
            for line in file:
                players.append(line.strip())
    PLAYERS  = players
    missing_data = {}
    missing_info = []
    raw_table = {}
    for player in PLAYERS:
        player = player.strip()
        row_data = { "player_name": player}
        # rows contains info about indicators, so for each player we get the info for that 
        # indicator and find the associated value
        for r in rows:
            indicator = r['indicator']
            filter_date = r['filter_date']
            reference_group = r['reference_group']
            if r['js'] == 1:
                # adapting to use jeff sackmann
                name = name_handling_JS(player)
                value = data_aggregation_JS(indicator, name, reference_group, cursor)
            else:
                value = extract_indicator(indicator, player, reference_group, filter_date, cursor)
            if value in [None, 'NA', '-']:
                # storing missng data for computation
                missing_info.append(indicator)
            # storing indicator and value to build the raw table
            row_data[indicator] = value
        if missing_info:
            missing_data[player] = missing_info
        missing_info = []
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
 