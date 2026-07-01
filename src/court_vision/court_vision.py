import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.patches import Polygon
import pandas as pd
import sqlite3
from gradpyent.gradient import Gradient
from matplotlib.axes import Axes
import numpy as np

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

def draw_polygon(ax: Axes, points: list[tuple[int, int]], color: str)-> None:
    """
    Draw a colored polygon representing a court zone.
    """
    polygon = Polygon(points,
            closed=True,
            edgecolor= 'white',
            linewidth=3, 
            facecolor=color
    )
    # Add the polygon to the current axes.
    ax.add_patch(polygon)

def write_text(ax: Axes, label: str, points: list[tuple[int, int]], percentage: int) -> None:
    """
    Write a text in the center of the polygon.
    """
    # Name of the area and the value. 
    text = label + f'\n{percentage}%'
    # Create 2 separate lists one with all the X coordinates, and one with all the Y coordinates. 
    xs, ys = zip(*points)
    # Compute the average x and y coordinates of the polygon vertices. 
    # Fast and simple way to obtain  a position close to the center of the polygon .
    cx = sum(xs) / len(xs)
    cy = sum(ys) / len(ys)
    # Add the text to the current axes. 
    ax.text(cx, cy, text,
            ha="center", va="center", color="white", fontsize=20)
    
def pull_data(players: list, indicators: list, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Retrieve the requested indicators for the specified players from the
    general table.
    Returns a DataFrame indexed by player name, with one row per player
    and one column per requested indicator.
    """
    # Safety fall. 
    if not players:
        return pd.DataFrame()
    # Add player name to the cols for clearity. 
    cols = ["player_name", *indicators]
    # Merging the columns to meet sqlite3 requirements. 
    columns = ','.join(f'"{col}"' for col in cols)
    placeholders = ','.join(f"?" for _ in players)
    query = f'''
        SELECT {columns} FROM general WHERE "player_name" in ({placeholders})
        '''
    return pd.read_sql_query(query, conn, params=players).set_index('player_name')

def from_number_to_color(gg: Gradient, percentage: int) -> str:
    """
    Map a percentage value to a color using the predefined gradient.
    The percentage is first normalized to the [0, 1] interval and then
    transformed with a gamma correction to increase the visual contrast
    between similar values.
    """
    # Normalize the percentage to the [0, 1] range.
    normalized = percentage / 100
    # Apply a gamma correction to emphasize color differences.
    normalized = normalized ** 0.4
    # Return the corresponding color from the gradient.
    return gg.get_gradient_series(series=[normalized], fmt="html")[0]

def render_court(img:np.ndarray, data: dict, player: str, gg: Gradient, zones: dict, zone_name: str) -> None:
    """
    Render and save a complete court visualization for a single player.

    For each court zone, the function retrieves the corresponding value,
    maps it to a color, draws the polygon, and places the associated text.
    """

    # Create a new figure for the current player.
    fig, ax = plt.subplots(figsize=(30, 30 * 808 / 1947))
    # Display the court template.
    ax.imshow(img)
    ax.axis("off")
    # Retrieve the player's statistics.
    row = data[player]
    for zone in zones.values():
        # Retrieve the geometry of the current zone.
        points = zone["points"]
        # Retrieve the display label.
        label = zone["label"]
        # Retrieve the corresponding database column.
        column = zone["column"]
        # Fetch the player's percentage for the current zone.
        percentage = round(row.get(column), 0)
        # Convert the percentage into the corresponding fill color.
        color = from_number_to_color(gg, percentage)
        draw_polygon(ax, points, color)
        write_text(ax, label, points, percentage)
    # Save the rendered visualization.
    fig.savefig(
        f"outputs/court_vision/{player}_{zone_name}.png",
        bbox_inches="tight",
        pad_inches=0,
    )
    plt.close(fig)

def main():
    gg = Gradient(gradient_start=START_COLOR, gradient_end=END_COLOR, opacity=1.0)
    conn = sqlite3.connect('data/db/tennis_abstract_new_version_merged_testing.db')
    players = ['GabrielDiallo','LorenzoSonego']
    img = mpimg.imread(IMAGE_PATH)
    visualizations = [
        ("serve_direction", SERVE_ZONES),
        ("return_depth", RETURN_ZONES),
] 
    for zone_name, zone in visualizations:
        indicators = [z['column'] for z in zone.values()]
        data = pull_data(players, indicators, conn).to_dict(orient='index')
        for pl in players:
            render_court(img, data, pl, gg, zone, zone_name)
   
main()