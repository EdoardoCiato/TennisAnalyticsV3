
import sqlite3
import re
from jinja2 import Template

# Mapping delle metriche per sezione con descrizioni
METRICS_MAPPING = {
    'serve': [
        ('1st Avg', '1st Speed', 'Velocità media prima di servizio'),
        ('1st T Avg', '1st T Speed', 'Velocità prima al T'),
        ('1st Wide Avg', '1st Wide Speed', 'Velocità prima a uscire'),
        ('2nd Avg', '2nd Speed', 'Velocità seconda di servizio'),
        ('2nd T Avg', '2nd T Speed', 'Velocità seconda al T'),
        ('2nd Wide Avg', '2nd Wide Speed', 'Velocità seconda a uscire'),
        ('1st %', '1st won %', '% punti vinti con la prima'),
        ('2nd %', '2nd won %', '% punti vinti con la seconda'),
        ('Deuce Aces %', 'Deuce Aces %', '% ace lato deuce'),
        ('Ad Aces %', 'Ad Aces %', '% ace lato vantaggio'),
        ('DF %', 'Double Faults %', '% doppi falli per punto'),
    ],
    'return': [
        ('RPW', 'Return Points won', '% punti vinti in risposta (RPW)'),
        ('BP Conv%', 'Break Pts converted', '% palle break convertite'),
        ('Deuce RPW%', 'Deuce Return Pts won%', '% punti risposta lato deuce'),
        ('Ad RPW%', 'Ad Return Pts won %', '% punti risposta lato ad'),
        ('Return shallow %', 'Return shallow %', '% risposte corte'),
        ('Return deep %', 'Return deep %', '% risposte profonde'),
        ('Return very deep %', 'Return very deep %', '% risposte molto profonde'),
        ('RiP %', 'Return in Play won %', '% punti vinti con risposta in gioco'),
    ],
    'rally': [
        ('FH Wnr Pts', 'Forehand Wnr Pts', '% winners di dritto per punto'),
        ('BH Wnr Pts', 'Backhand Wnr Pts', '% winners di rovescio per punto'),
        ('Winners', 'Winners', '% winners totali per punto'),
        ('UFE %', 'Unforced Errors', '% errori non forzati per punto'),
        ('IO Wnr%', 'Inside Out Winners', '% winners inside-out'),
        ('FH DTL Wnr%', 'Forehand Down the Line Winners', '% winners DTL di dritto'),
        ('BH DTL Wnr%', 'Backhand Down the Line Winners', '% winners DTL di rovescio'),
    ],
    'attitude': [
        ('BreakBack%', 'Break Back%', '% contro-break dopo aver perso il servizio'),
        ('Return Aggressive', 'Return Aggressive', 'Indice aggressività in risposta'),
        ('Rally Aggressive', 'Rally Aggressive', 'Indice aggressività negli scambi'),
        ('SvStayMatch', 'Serve StayMatch won', '% game vinti al servizio per restare'),
        ('SvForMatch', 'Serve ForMatch won', '% game vinti al servizio per chiudere'),
        ('Consol%', 'Break Consolidation %', '% game vinti dopo aver brekkato'),
        ('TB%', 'Tie Break won', '% tiebreak vinti'),
        ('Dominance Ratio', 'Dominance Ratio', 'Rapporto RPW/persi al servizio'),
    ],
    'efficiency': [
        ('SPW', 'Serve Points won', '% punti vinti al servizio'),
        ('Hld%', 'Serve Games hold %', '% game servizio tenuti'),
        ('RPW', 'Return Points won', '% punti vinti in risposta'),
        ('Break vs Games with Break Pts', 'Break vs Games with Break Pts', '% conversione BP'),
        ('BP Games', 'Games with Break Pts', '% game risposta con almeno una BP'),
        ('Winners', 'Winners', '% winners per punto'),
        ('UFE %', 'Unforced Errors', '% errori non forzati per punto'),
        ('Ratio', 'Ratio Winners / Unforced', 'Rapporto W/UE'),
    ],
}
def inject_overview(players, cursor):
    player1 = players[1]
    player2 = players[0]

    data = {
        'player1': player_info(player1, cursor),
        'player2': player_info(player2, cursor)
    }

    return data

def inject_glossary(players, cursor):
    """Extract detailed metrics data for glossary report"""
    player1 = players[1]
    player2 = players[0]

    # Get basic player info
    p1_info = player_info(player1, cursor)
    p2_info = player_info(player2, cursor)

    # Get all columns from GENERAL table
    cursor.execute('PRAGMA table_info("GENERAL")')
    columns = [row[1] for row in cursor.fetchall()]

    # Get ATP averages
    cursor.execute('SELECT * FROM "global_averages" LIMIT 1')
    atp_row = cursor.fetchone()
    atp_dict = dict(zip(columns, atp_row)) if atp_row else {}

    # Get player 1 data
    cursor.execute('SELECT * FROM GENERAL WHERE player_name = ?', (player1,))
    p1_row = cursor.fetchone()
    p1_dict = dict(zip(columns, p1_row)) if p1_row else {}

    # Get player 2 data
    cursor.execute('SELECT * FROM GENERAL WHERE player_name = ?', (player2,))
    p2_row = cursor.fetchone()
    p2_dict = dict(zip(columns, p2_row)) if p2_row else {}

    # Organize metrics by section with descriptions
    metrics_by_section = {}
    for section, metrics_list in METRICS_MAPPING.items():
        metrics_by_section[section] = []
        for db_col, display_name, description in metrics_list:
            metrics_by_section[section].append({
                'db_col': db_col,
                'name': display_name,
                'description': description,
                'atp': atp_dict.get(db_col, '-'),
                'player1': p1_dict.get(db_col, '-'),
                'player2': p2_dict.get(db_col, '-'),
            })

    data = {
        'player1': p1_info,
        'player2': p2_info,
        'metrics': metrics_by_section,
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

def generate_report(data, template_path):
    """Render Jinja2 template with provided data"""
    with open(template_path) as f:
        template = Template(f.read())
    return template.render(data)

def main():
    conn = sqlite3.connect("data/db/tennis_abstract_new_version_merged_testing.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    players = ["MatteoArnaldi", 'LorenzoSonego']

    # Generate overview
    overview_data = inject_overview(players, cursor)
    overview_data["comments"] = {}
    overview_data['overview'] = {}
    overview_html = generate_report(overview_data, "data/templates/overview_template.html")
    with open('outputs/html/overview_templateV2.html', 'w') as f:
        f.write(overview_html)
    print("✅ Overview generated: outputs/html/overview_templateV2.html")

    # Generate glossary
    glossary_data = inject_glossary(players, cursor)
    glossary_html = generate_report(glossary_data, "data/templates/glossary.html")
    with open('outputs/html/glossary.html', 'w') as f:
        f.write(glossary_html)
    print("✅ Glossary generated: outputs/html/glossary.html")

    conn.close()

if __name__ == "__main__":
    main()