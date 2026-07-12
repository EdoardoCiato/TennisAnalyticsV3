import sqlite3
import re
from jinja2 import Template

from src.helpers.helper_functions import fetch_coefficients_data
def inject_overview(players, cursor):
    player1 = players[1]
    player2 = players[0]

    data = {
        'player1': player_info(player1, cursor),
        'player2': player_info(player2, cursor)
    }

    return data

def player_info(player, cursor):
    first_name, last_name = re.findall('[A-Z][^A-Z]*', player)
    indicators = ["ranking", 'matches_analyzed', 'Winners', 'Break %', "Ad Service Pts won %", "Deuce Return Pts won%"]
    columns = ','.join(f'"{col}"' for col in indicators)
    query = f'SELECT {columns} FROM GENERAL WHERE player_name = ?'
    cursor.execute(query, (player,))
    values = cursor.fetchone()
    if values is None:

        return None

    keys = ["ranking", "matches_analyzed", "card1", "card2", "card3", "card4", 'serve_index', ]
    player_data = dict(zip(keys, values))
    player_data["first_name"] = first_name
    player_data["last_name"] = last_name
    player_data["name"] = first_name + ' ' + last_name
    player_data['db_name'] = first_name + last_name

    return player_data

def generate_report(data):
    with open("data/templates/overview_template.html") as f:
        template = Template(f.read())

    return template.render(data)

def main():
    conn = sqlite3.connect("data/db/tennis_abstract_new_version_merged_testing.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    players = [ "MatteoArnaldi", 'LorenzoSonego']
    data = (inject_overview(players, cursor))
    data["comments"] = {}
    data['overview'] = {}
    coefficients_df = fetch_coefficients_data(conn, players)
    for player_key in ["player1", "player2"]:
        db_name = data[player_key]["db_name"]
        for coeff_name, coeff_value in coefficients_df[db_name].items():
            coeff_name = coeff_name.lower()+'_index'
            data[player_key][coeff_name] = round(coeff_value,1)
    html = generate_report(data)
    print(data)
    with open ('outputs/html/overview_templateV3.html', 'w') as f:
        f.write(html)

main()