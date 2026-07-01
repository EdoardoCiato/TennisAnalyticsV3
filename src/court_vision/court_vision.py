import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.patches import Polygon
import pandas as pd
import sqlite3
from gradpyent.gradient import Gradient

START_COLOR = '#F8FBFF'
END_COLOR = '#001B44'  

IMAGE_PATH = 'data/templates/image.png'
SERVE_ZONES = {
    "deuce_T": {
        "label": "T",
        "column": "Serve deuce side t %",
        "points": [
            (978, 411),(977, 210),
            (1112, 210),(1168, 411),
        ],
    },

    "deuce_body": {
        "label": "Body",
        "column": "Serve deuce side middle %",
        "points": [
            (1168, 411),(1112, 210),
            (1260, 207),(1370, 411),
        ],
    },

    "deuce_wide": {
        "label": "Wide",
        "column": "Serve deuce side wide %",
        "points": [
            (1370, 411),(1260, 207),
            (1400, 204),(1576, 411),
        ],
    },

    "ad_wide": {
        "label": "Wide",
        "column": "Serve ad side wide %",
        "points": [
            (368, 411),(574, 411),
            (684, 205),(544, 204),
        ],
    },

    "ad_body": {
        "label": "Body",
        "column": "Serve ad side middle %",
        "points": [
            (574, 411),(684, 205),
            (832, 207),(776, 411),
        ],
    },

    "ad_T": {
        "label": "T",
        "column": "Serve ad side t %",
        "points": [
            (832, 207),(776, 411),
            (966, 411),(967, 210),
        ],
    },
}

RETURN_ZONES = { 
       "shallow": {
        "label": "shallow",
        "column": "Return shallow %",
        "points": [
            (544, 204),(1400, 204),
            (1576, 411),(368, 411),
        ],
    },

     "deep": {
         "label": "deep",
         "column": "Return deep %",
         "points": [
            (240, 560),(1707, 560),
            (1576, 411),(368, 411),
         ],
     },

     "very_deep": {
         "label": "very deep",
         "column": "Return very deep %",
         "points": [
             (240, 560),(1707, 560),
             (1881, 765),(69, 765),
         ],
     }
}

def draw_polygon(ax, points, color):
    polygon = Polygon(points,
            closed=True,
            edgecolor= 'white',
            linewidth=3, 
            facecolor=color
    )

    ax.add_patch(polygon)

def write_text(ax, label, points, percentage):
    text = label + f'\n{percentage}%'
    xs, ys = zip(*points)
    cx = sum(xs) / len(xs)
    cy = sum(ys) / len(ys)
    ax.text(cx, cy, text,
            ha="center", va="center", color="white", fontsize=20)
    
def pull_data(players, indicators, conn):
    if not players:
        return pd.DataFrame()
    cols = ["player_name", *indicators]
    columns = ','.join(f'"{col}"' for col in cols)
    placeholders = ','.join(f"?" for _ in players)
    query = f"""
    SELECT {columns}
    FROM general
    WHERE "player_name" IN ({placeholders})
    """
    query = f'''
    SELECT {columns} FROM general WHERE "player_name" in ({placeholders})'''

    return pd.read_sql_query(query, conn, params=players).set_index('player_name')

def from_number_to_color(gg, percentage):
    normalized = percentage / 100
    normalized = normalized ** 0.4
    return gg.get_gradient_series(series=[normalized], fmt='html')[0]

def court_renderer(img, data, player, gg, zones, zone_name):
    fig, ax = plt.subplots(figsize=(30, 30 * 808 / 1947))
    ax.imshow(img)
    ax.axis('off')
    row = data[player]
    for zone in zones.values():
            points = zone['points']
            label = zone['label']
            column = zone['column']
            percentage = round(row.get(column),0)
            color = from_number_to_color(gg, percentage)
            draw_polygon(ax, points,color)
            write_text(ax, label, points, percentage)

    plt.savefig(f'outputs/court_vision/{player}_{zone_name}.png', bbox_inches="tight", pad_inches=0)
    plt.show()

def main():
    gg = Gradient(gradient_start=START_COLOR, gradient_end=END_COLOR, opacity=1.0)
    conn = sqlite3.connect('data/db/tennis_abstract_new_version_merged_testing.db')
    players = ['GabrielDiallo', 'LorenzoSonego']
    img = mpimg.imread(IMAGE_PATH)
    visualizations = [
        ("serve_direction", SERVE_ZONES),
        ("return_depth", RETURN_ZONES),

]
    # fig = the figure, the entire window
    # ax = Axes, where you actually draw
    # we need both because a figure may contain multiple axes. 
    for zone_name, zone in visualizations:
        indicators = [z['column'] for z in zone.values()]
        data = pull_data(players, indicators, conn).to_dict(orient='index')
        for pl in players:
            court_renderer(img, data, pl, gg, zone, zone_name)
   
main()