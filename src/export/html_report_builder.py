from __future__ import annotations
from jinja2 import Template
from http.server import HTTPServer, SimpleHTTPRequestHandler
from functools import partial
from threading import Thread
import sqlite3
import re
from playwright.sync_api import sync_playwright, Playwright
from src.helpers.helper_functions import fetch_coefficients_data
from src.export.radar_chart import radar_chart
import pandas as pd

# TODO: check that the radar exists before moving forward

COEFFICIENTS_CATEGORIES = ["Serve", "Efficiency", "Attitude", "Rally", "Return"]
VERSION = 1
HTML_PATH = f'html/overview_templateV{VERSION}.html'

def run(playwright: Playwright):
     # Open and launch web browser
     chromium = playwright.chromium 
     browser = chromium.launch()
     page = browser.new_page()
     # Access the page where the html is saved. 
     page.goto(f'http://localhost:8000/{HTML_PATH}')
     # Save the html page as a pdf file
     page.pdf(path=f'outputs/pdf/test{VERSION}.pdf', print_background=True, width='1100',height='1000' )

def create_http(folder: str, server_class=HTTPServer) -> HTTPServer:
    # Server address is a tupple where the first element represents the network address to accept connections on ('' means
    # all of them) and the port (8000)
    server_address = ('', 8000)
    # It handles incoming request, here we make sure it always has a root. 
    handler = partial(SimpleHTTPRequestHandler, directory = folder)
    # Create the server. 
    httpd = server_class(server_address, handler)
    return httpd

def run_server(httpd: HTTPServer):
    # Makes the server run as long as needed.
    httpd.serve_forever()

def inject_overview(players:list, cursor:sqlite3.Cursor, df:pd.DataFrame) -> dict:
    # Assign from the players list the two players individually. 
    player1 = players[1]
    player2 = players[0]
    # Build the data dictionary with player info for each specific player for jinja injection.
    data = {
        'player1': player_info(player1, cursor),
        'player2': player_info(player2, cursor)
    }
    # Transpose and remove index so that we can filter by player. 
    df = df.T.reset_index()
    # Build the radar chart for the coefficients. 
    central_radar = radar_chart(df, player1, player2, COEFFICIENTS_CATEGORIES)
    # Add the radar chart to the data dictionary. 
    data['overview'] = {'central_radar':central_radar}
    return data

def player_info(player: str, cursor: sqlite3.Cursor) -> dict | None:
    # Split the full name in first and last name. (E.g., from LorenzoSonego to Lorenzo , Sonego)
    first_name, last_name = re.findall('[A-Z][^A-Z]*', player)
    # Overview indicators
    # TODO: decide if keep them fixed or do something different. 
    indicators = ["ranking", 'matches_analyzed', 'Winners', 'Break %', "Ad Service Pts won %", "Deuce Return Pts won%"]
    columns = ','.join(f'"{col}"' for col in indicators)
    # Pull the data from sqlite3 db. 
    # TODO: create a general function to apply to every script to fetch data from sqlite3. 
    query = f'SELECT {columns} FROM GENERAL WHERE player_name = ?'
    cursor.execute(query, (player,))
    values = cursor.fetchone()
    if values is None:

        return None
    # Names of the placeholders in the html. 
    keys = ["ranking", "matches_analyzed", "card1", "card2", "card3", "card4", 'serve_index', ]
    # Creating a dict by matching the placeholders with the actual values.
    player_data = dict(zip(keys, values))
    # Add further info to the player data dict
    player_data["first_name"] = first_name
    player_data["last_name"] = last_name
    player_data["name"] = first_name + ' ' + last_name
    player_data['db_name'] = first_name + last_name

    return player_data

def generate_report(data: dict)-> str:
    # Open the template and fill with the information in data. 
    with open("data/templates/overview_template.html") as f:
        # f.read() pulls the entire file's contents out as one big string 
        # Template is a jinja class that takes the raw file and parses it . Basically it finds the placeholders.
        template = Template(f.read())
    
    # template.render actually fills the data. 

    return template.render(data)

def main():
    # Create connection. 
    conn = sqlite3.connect("data/db/tennis_abstract_new_version_merged_testing.db")
    # fetching info row by row as a dictionary. 
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    players = [ "MatteoArnaldi", 'LorenzoSonego']
    # Fetch the coefficient df. 
    coefficients_df = fetch_coefficients_data(conn, players)
    # Build the data dictionary for injection. 
    data = (inject_overview(players, cursor, coefficients_df ))
    # Add personal comments. 
    data["comments"] = {
        'what_to_do1': "Sii aggressivo sulla seconda di servizio: Arnaldi fatica a contenere il ritmo quando riesci ad ",
        'what_to_do2': "Gioca con profondità sul rovescio incrociato: è il colpo che gli crea più difficoltà, soprattutto sul lato sinistro",
        'what_to_do3': "Sfrutta le palle corte per salire a rete: la sua risposta perde efficacia quando lo costringi ",
        'key_points_recommendation': "Attenzione ai primi due game di ogni set: Arnaldi parte spesso più aggressivo e cerca il break immediato per prendere fiducia",
        'dont1': "Non concedergli angoli sul dritto in cross: da quella posizione produce la maggior parte dei suoi vincenti",
        'dont2': "Evita scambi lunghi e ripetitivi da fondo campo: è più paziente e commette meno errori nei rally prolungati",
    }
    # Pull the values from the coefficient df and add them to the data dictionary. 
    for player_key in ["player1", "player2"]:
        db_name = data[player_key]["db_name"]
        for coeff_name, coeff_value in coefficients_df[db_name].items():
            coeff_name = coeff_name.lower()+'_index'
            data[player_key][coeff_name] = round(coeff_value,1)

    data['player1']['playstyle1'] = "Giocatore da fondo offensivo con dritto potente e buona variazione di traiettorie"
    data['player1']['playstyle2'] = "Servizio efficace su entrambi i lati, con buona percentuale di prime in campo"
    data['player1']['playstyle3'] = "Discreta propensione a salire a rete dopo approcci profondi"
    data['player1']['playstyle4'] = "Buona resistenza fisica negli scambi lunghi, soprattutto nei momenti chiave del match"

    data['player2']['playstyle1'] = "Giocatore molto solido da fondo campo, con pochi errori non forzati"
    data['player2']['playstyle2'] = "Rovescio bimane preciso e affidabile, spesso usato per cambiare direzione"
    data['player2']['playstyle3'] = "Buona copertura del campo grazie a spostamenti rapidi laterali"
    data['player2']['playstyle4'] = "Tende a giocare in modo più difensivo nei momenti di pressione"

    html = generate_report(data)
    # Create a new file and write in it the content of the injected html file. 
    with open (f'outputs/{HTML_PATH}', 'w') as f:
        f.write(html)

    # EXPORTING THE HTML AS PDF

    # Create the serve with root the outputs directory 
    httpd = create_http('outputs/')
    # Create a thread to keep the server running as long as needed.
    thread = Thread(daemon=True, target = run_server, args=(httpd, ))
    # We use a thread to keep the server open until the playwright function is complete. 
    thread.start()
    # playwright is your entry point to each browser engine Playwright can control
    # sync_playwright() sets up the underlying Playwright driver 
    with sync_playwright() as playwright:
        # Run the playwright function to download the pdf. 
        run(playwright)
    # Shutdown the server. 
    httpd.shutdown()

main()