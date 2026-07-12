import numpy as np
import matplotlib.pyplot as plt
from src.helpers.helper_functions import fetch_coefficients_data
import sqlite3

def radar_chart(df, player1, player2, categories, output_path=None):
    # Extract values in the same order as categories

    # iloc takes the first row from the df, to list because otherwise it a pandas series
    values_player1 = df.loc[df["player_name"] == player1, categories].iloc[0].tolist()
    values_player2 = df.loc[df["player_name"] == player2, categories].iloc[0].tolist()

    # Close the polygon
    #. we add at the end the first element to close the polygon. 
    values_player1 = values_player1 + [values_player1[0]]
    values_player2 = values_player2 + [values_player2[0]]

    # Angles for each axis
    angles = np.linspace(0, 2 * np.pi, len(categories), endpoint=False).tolist()
    # Add the angle to close the polygon. 
    angles.append(angles[0])

    # Figure
    # polar = True because radar chart is built with polar coordinates ( angles and radius) rather than x,y coordinates. 
    fig, ax = plt.subplots(figsize=(8, 8),  facecolor="#f7f3ea", subplot_kw=dict(polar=True))
    ax.set_facecolor("#f7f3ea")
    
    # Put first axis on the right and go clockwise
    ax.set_theta_offset(0)

    # Grid and limits
    ax.set_ylim(0, 100)
    ax.set_yticks([20, 40, 60, 80, 100])
    ax.set_yticklabels([])
    ax.grid(color="grey", alpha=0.25, linewidth=1)
    radius = [0, 20, 40, 60, 80, 100]
    for a in angles:
        ang = [a] * len(radius)
        ax.plot(ang, radius , linewidth=1.5, color='lightgrey', zorder=1)
    for r in radius:
        rad = [r] * len(angles)
        ax.plot(angles, rad , linewidth=1.5, color='lightgrey', zorder=1 )




    # Labels
    # We exclude the last element because we included just to close the Polygon. 
    # set_xtcicks sets the position. 
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories, fontsize=14, fontweight="bold", color="#2b4c7e")

    # Plot players
    ax.plot(angles, values_player1, color="#4fc3f7", linewidth=2, marker="o", label=player1)
    ax.fill(angles, values_player1, color="#4fc3f7", alpha=0.20, zorder= 3)

    ax.plot(angles, values_player2, color="#1b5e20", linewidth=2, marker="o", label=player2)
    ax.fill(angles, values_player2, color="#1b5e20", alpha=0.20, zorder = 3)

    # Legend
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.08), frameon=False)

    # Clean frame
    ax.spines["polar"].set_visible(False)
    ax.grid(False)
    

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close(fig)
    else:
        plt.show()


players = [ "GabrielDiallo", 'LorenzoSonego']
conn = sqlite3.connect("data/db/tennis_abstract_new_version_merged_testing.db")
categories = ["Serve", "Efficiency", "Attitude", "Rally", "Return"]
coefficients_df = fetch_coefficients_data(conn, players).T.reset_index()
radar_chart(coefficients_df, "LorenzoSonego", "GabrielDiallo", categories, "radar.png")