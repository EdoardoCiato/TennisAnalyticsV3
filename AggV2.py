from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import percentileofscore

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR
INPUT_DB = BASE_DIR / "aggressiveness_coeff.db"
RAW_DB = PROJECT_ROOT / "tennis_abstract_new_version.db"
MERGED_DB = PROJECT_ROOT / "tennis_abstract_new_version_merged.db"
OUTPUT_DB = BASE_DIR / "aggressiveness_v2.db"
OUTPUT_TABLE = "aggressiveness_index"
RALLY_WIN_COLS = ["1-3_W%", "4-6_W%", "7-9_W%", "10+_W%"]
SURFACES = ["Hard", "Clay", "Grass"]

# Pesi decrescenti per lunghezza scambio (sommano a 1.0).
# valori per cambiare l'importanza relativa di ciascuna fascia.
RALLY_LENGTH_WEIGHTS: dict[str, float] = {
    "1-3_W%":  0.40,  # primo attacco / servizio diretto
    "4-6_W%":  0.30,  # scambio corto
    "7-9_W%":  0.20,  # scambio medio-lungo
    "10+_W%":  0.10,  # grinder / resistenza
}

TOPICS: dict[str, dict[str, int]] = {
    "Serve": {
        "Serve | 1stIn": +1,
        "Serve | 1st%": +1,
        "Serve | 2nd%": +1,
        "Serve | A%": +1,
        "Serve | Hld%": +1,
        "Serve | SPW": +1,
        "Serve | DF%": -1,
    },
    "Rally": {
        "Rally | UFE/Pt": -1,
        "Rally | RallyAgg": +1,
        "Rally | Ratio": +1,
        "Rally | FH_Wnr/Pt": +1,
        "Rally | BH_Wnr/Pt": +1,
    },
    "Attitude": {
        "Attitude | SvStayMatch": +1,
        "Attitude | ReturnAgg": +1,
        "Attitude | RallyAgg": +1,
        "Attitude | Consol%": +1,
        "Attitude | TB%": +1,
        "Attitude | BreakBack%": +1,
        "Attitude | BP_Conv": +1,
    },
    "Tactics": {
        "Tactics | Drop:_Freq": 0,
        "Tactics | Net_Freq": +1,
        "Tactics | SnV_Freq": +1,
        "Tactics | crosscourt": 0,
        "Tactics | down_the_line": +1,
        "Tactics | inside_in": +1,
        "Tactics | inside_out": +1,
    },
    "Efficiency": {
        "Efficiency | Ratio": +1,
        "Efficiency | RPW": +1,
        "Efficiency | Wnr/Pt": +1,
        "Efficiency | SPW": +1,
        "Efficiency | Hld%": +1,
        "Efficiency | BP_Conv/BPG": +1,
        "Efficiency | BP_Games": +1,
    },
}

PERCENT_RE = re.compile(r"(-?\d+(?:[.,]\d+)?)\s*%")
NUMBER_RE = re.compile(r"-?\d+(?:[.,]\d+)?")


def parse_numeric(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).replace("\xa0", " ").strip()
    if text in ("", "NA", "-", "None", "NULL"):
        return None

    percent_match = PERCENT_RE.search(text)
    if percent_match:
        return float(percent_match.group(1).replace(",", "."))

    number_match = NUMBER_RE.search(text)
    if number_match:
        return float(number_match.group(0).replace(",", "."))

    return None


def select_rally_table(table_names: list[str]) -> str | None:
    if not table_names:
        return None

    preferred = [name for name in table_names if "RallyAll_matchesGlossary" in name]
    if preferred:
        return sorted(preferred, key=len, reverse=True)[0]

    preferred = [name for name in table_names if "RallyGlossary" in name]
    if preferred:
        return sorted(preferred, key=len, reverse=True)[0]

    return sorted(table_names, key=len, reverse=True)[0]


def _load_rally_win_raw(raw_db: Path, player_keys: list[str]) -> pd.DataFrame:
    """Legge i valori grezzi 1-3_W%, 4-6_W%, 7-9_W%, 10+_W% per ogni giocatore."""
    conn = sqlite3.connect(raw_db)
    rows = []
    try:
        for player_key in player_keys:
            table_rows = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name LIKE ? "
                "ORDER BY name",
                (f"{player_key}_Match_Charting_Project_Rally%",),
            ).fetchall()
            table_name = select_rally_table([row[0] for row in table_rows])

            row = {"player_key": player_key}
            for col in RALLY_WIN_COLS:
                row[col] = None

            if table_name:
                quoted_cols = ", ".join(f'"{col}"' for col in RALLY_WIN_COLS)
                career_df = pd.read_sql_query(
                    f'SELECT {quoted_cols} FROM "{table_name}" '
                    f'WHERE "Match" LIKE \'Career%\' '
                    f'LIMIT 1',
                    conn,
                )
                if not career_df.empty:
                    for col in RALLY_WIN_COLS:
                        numeric = parse_numeric(career_df.iloc[0][col])
                        if numeric is not None:
                            row[col] = round(float(numeric), 4)

            rows.append(row)
    finally:
        conn.close()

    return pd.DataFrame(rows)


def compute_rally_length_scores(raw_db: Path, player_keys: list[str]) -> pd.DataFrame:
    """Calcola i percentili per ciascuna fascia di scambio e il coeff_rally_length
    come media pesata secondo RALLY_LENGTH_WEIGHTS."""
    if not raw_db.exists():
        raise FileNotFoundError(
            f"DB raw non trovato: {raw_db}\n"
            "Necessario per leggere le statistiche Match Charting Project Rally."
        )

    raw = _load_rally_win_raw(raw_db, player_keys)

    # calcola percentile per ogni fascia
    for col in RALLY_WIN_COLS:
        pct_col = f"pct_{col}"
        valid_vals = raw[col].dropna().values
        scores = []
        for val in raw[col]:
            if pd.isna(val) or val is None:
                scores.append(50.0)
            else:
                scores.append(round(float(percentileofscore(valid_vals, val, kind="mean")), 1))
        raw[pct_col] = scores

    # coeff_rally_length: media pesata dei percentili
    def _weighted_rally(row):
        return round(
            sum(RALLY_LENGTH_WEIGHTS[col] * row[f"pct_{col}"] for col in RALLY_WIN_COLS),
            1,
        )

    raw["coeff_rally_length"] = raw.apply(_weighted_rally, axis=1)
    return raw


def compute_surface_scores(merged_db: Path, player_keys: list[str]) -> pd.DataFrame:
    if not merged_db.exists():
        raise FileNotFoundError(f"DB merged non trovato: {merged_db}")

    conn = sqlite3.connect(merged_db)
    raw = pd.read_sql_query(
        'SELECT "__player__", "Split", "M", "Win%" FROM group_007 WHERE "Split" IN (?, ?, ?)',
        conn,
        params=SURFACES,
    )
    conn.close()

    raw["win_pct"] = raw["Win%"].apply(parse_numeric)
    raw["matches"] = pd.to_numeric(raw["M"], errors="coerce").fillna(0).astype(int)

    pivot_pct = (
        raw.pivot_table(index="__player__", columns="Split", values="win_pct", aggfunc="first")
        .reset_index()
        .rename(columns={"__player__": "player_key"})
    )
    pivot_pct.columns.name = None

    pivot_m = (
        raw.pivot_table(index="__player__", columns="Split", values="matches", aggfunc="first")
        .reset_index()
        .rename(columns={surf: f"{surf}_M" for surf in SURFACES})
        .rename(columns={"__player__": "player_key"})
    )
    pivot_m.columns.name = None

    # calcola i percentili solo sui giocatori top200
    pivot_pct = pivot_pct[pivot_pct["player_key"].isin(player_keys)].copy()

    result = pivot_pct[["player_key"]].copy()
    for surf in SURFACES:
        coeff_col = f"coeff_{surf.lower()}"
        if surf not in pivot_pct.columns:
            result[coeff_col] = 50.0
            continue
        valid_vals = pivot_pct[surf].dropna().values
        scores = []
        for val in pivot_pct[surf]:
            if pd.isna(val):
                scores.append(50.0)
            else:
                scores.append(round(float(percentileofscore(valid_vals, val, kind="mean")), 1))
        result[coeff_col] = scores

    # aggiunge le colonne M per usarle nella media pesata
    result = result.merge(pivot_m[["player_key"] + [f"{s}_M" for s in SURFACES]], on="player_key", how="left")
    for surf in SURFACES:
        result[f"{surf}_M"] = result[f"{surf}_M"].fillna(0).astype(int)

    # garantisce una riga per ogni player_key, anche se mancante in group_007
    all_players = pd.DataFrame({"player_key": player_keys})
    result = all_players.merge(result, on="player_key", how="left")
    for surf in SURFACES:
        result[f"coeff_{surf.lower()}"] = result[f"coeff_{surf.lower()}"].fillna(50.0)
        result[f"{surf}_M"] = result[f"{surf}_M"].fillna(0).astype(int)

    return result


def compute_rank_scores(input_db: Path) -> pd.DataFrame:
    if not input_db.exists():
        raise FileNotFoundError(
            f"DB sorgente non trovato: {input_db}\n"
            "Esegui prima: python .\\Agg_Coeff\\build_aggressiveness_coeff.py"
        )

    conn = sqlite3.connect(input_db)
    df = pd.read_sql_query("SELECT * FROM glossary_top200", conn)
    conn.close()

    stat_cols = [col for topic in TOPICS.values() for col in topic]

    for col in stat_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"   Giocatori: {len(df)}")

    rank_df = df[["player_name", "player_key", "ranking_order"]].copy()

    for col in stat_cols:
        if col not in df.columns:
            rank_df[col] = np.nan
            continue

        valid_vals = df[col].dropna().values
        ranks = []
        for val in df[col]:
            if pd.isna(val):
                ranks.append(np.nan)
            else:
                ranks.append(percentileofscore(valid_vals, val, kind="mean"))
        rank_df[col] = ranks

    results = []
    missing_counts = {}

    for _, player in rank_df.iterrows():
        row = {
            "player_name": player["player_name"],
            "player_key": player["player_key"],
            "ranking_order": player["ranking_order"],
        }
        topic_scores = []

        for topic_name, stats in TOPICS.items():
            percentiles = []
            for col, parity in stats.items():
                if parity == 0 or col not in rank_df.columns:
                    continue

                p = player[col]
                if pd.isna(p):
                    p = 50.0
                    missing_counts[col] = missing_counts.get(col, 0) + 1
                elif parity == -1:
                    p = 100.0 - p

                percentiles.append(p)

            topic_score = round(float(np.mean(percentiles)), 1) if percentiles else 50.0
            row[f"coeff_{topic_name.lower()}"] = topic_score
            topic_scores.append(topic_score)

        row["coeff_global"] = round(float(np.mean(topic_scores)), 1)
        results.append(row)

    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values("coeff_global", ascending=False).reset_index(drop=True)
    result_df["rank_aggressiveness"] = range(1, len(result_df) + 1)

    rally_length_df = compute_rally_length_scores(RAW_DB, result_df["player_key"].tolist())
    result_df = result_df.merge(rally_length_df, on="player_key", how="left")

    surface_scores_df = compute_surface_scores(MERGED_DB, result_df["player_key"].tolist())
    result_df = result_df.merge(surface_scores_df, on="player_key", how="left")

    # coeff_surface: media pesata per numero di match su ciascuna superficie
    def _weighted_surface(row):
        pairs = [(row[f"{s}_M"], row[f"coeff_{s.lower()}"]) for s in SURFACES]
        total_m = sum(m for m, _ in pairs if m > 0)
        if total_m > 0:
            return round(sum(m * c for m, c in pairs if m > 0) / total_m, 1)
        return round(float(np.mean([c for _, c in pairs])), 1)

    result_df["coeff_surface"] = result_df.apply(_weighted_surface, axis=1)

    # ricalcola coeff_global includendo coeff_surface e coeff_rally_length
    topic_cols = [
        "coeff_serve", "coeff_rally", "coeff_attitude",
        "coeff_tactics", "coeff_efficiency",
        "coeff_surface", "coeff_rally_length",
    ]
    result_df["coeff_global"] = result_df[topic_cols].mean(axis=1).round(1)
    result_df = result_df.sort_values("coeff_global", ascending=False).reset_index(drop=True)
    result_df["rank_aggressiveness"] = range(1, len(result_df) + 1)

    if missing_counts:
        print("   Missing values sostituiti con 50 (media):")
        for col, cnt in sorted(missing_counts.items(), key=lambda item: -item[1]):
            print(f"     {col}: {cnt} giocatori")

    return result_df


def print_results(result_df: pd.DataFrame) -> None:
    print("\n" + "-" * 97)
    print(f"{'#':>4}  {'Giocatore':<30} {'Serve':>6} {'Rally':>6} {'Attit.':>6} {'Tact.':>6} {'Effic.':>6} {'Surf.':>6} {'RallyL':>7} {'Global':>7}")
    print("-" * 97)

    for _, row in result_df.head(20).iterrows():
        print(
            f"{int(row['rank_aggressiveness']):>4}  "
            f"{row['player_name']:<30} "
            f"{row['coeff_serve']:>6.1f} "
            f"{row['coeff_rally']:>6.1f} "
            f"{row['coeff_attitude']:>6.1f} "
            f"{row['coeff_tactics']:>6.1f} "
            f"{row['coeff_efficiency']:>6.1f} "
            f"{row['coeff_surface']:>6.1f} "
            f"{row['coeff_rally_length']:>7.1f} "
            f"{row['coeff_global']:>7.1f}"
        )


def main() -> None:
    print("=" * 55)
    print("  Aggressiveness Coefficient v2 - Rank-based")
    print("=" * 55)
    print(f"   Input : {INPUT_DB}")
    print(f"   Output: {OUTPUT_DB}")

    result_df = compute_rank_scores(INPUT_DB)

    conn = sqlite3.connect(OUTPUT_DB)
    result_df.to_sql(OUTPUT_TABLE, conn, if_exists="replace", index=False)
    conn.commit()
    conn.close()

    print(f"\nOK '{OUTPUT_TABLE}' salvata - {len(result_df)} giocatori\n")
    print("Top 20 - Aggressiveness Index (0-100, rank-based):")
    print_results(result_df)
    print(f"\nOK DB salvato: {OUTPUT_DB}")


if __name__ == "__main__":
    main()
