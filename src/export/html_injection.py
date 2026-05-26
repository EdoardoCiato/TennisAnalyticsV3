import sqlite3
import re
from jinja2 import Template
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

    keys = ["ranking", "matches_analyzed", "card1", "card2", "card3", "card4"]
    player_data = dict(zip(keys, values))
    player_data["first_name"] = first_name
    player_data["last_name"] = last_name
    player_data["name"] = first_name + ' ' + last_name

    return player_data

def generate_report(data):
    with open("overview_template.html") as f:
        template = Template(f.read())

    return template.render(data)



def main():
    conn = sqlite3.connect("tennis_abstract_new_version_merged_testing.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    data = (inject_overview([ "MatteoArnaldi", 'LorenzoSonego'], cursor))
    data["comments"] = {}
    data['overview'] = {}
    html = generate_report(data)
    with open ('overview_templateV1.html', 'w') as f:
        f.write(html)


# data = {
#     "players": {
#         "player1": "Lorenzo Sonego",
#         "player2": "Mariano Navone"
#     },

#     "serve": [
#         {"label": "Indice Servizio", "p1": 58.1, "p2": 24.1, "delta": "+34.0"},
#         {"label": "Efficienza", "p1": 56.8, "p2": 43.6, "delta": "+13.2"}
#     ],
# }

main()