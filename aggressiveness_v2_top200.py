










"""
Aggressiveness Coefficient v2 — Top 200 ATP reference
======================================================

Calcola i coefficienti di aggressivita per due giocatori usando come campione
di riferimento il top 200 ATP su tutte e 59 le metriche disponibili.

Fonti delle distribuzioni
--------------------------
  "agg:" aggressiveness_coeff.db / glossary_top200
         33 metriche classiche, 192-193 valori per colonna.
  "gen:" tennis_abstract_merged.db / general
         6 metriche extra con distribuzione >= 100 giocatori:
         Break %, Return shallow/deep/very deep/unforced %, Dominance Ratio.
  None   metriche senza distribuzione top200: score lineare ATP-relativo
         (player/ATP_avg * 50), peso dimezzato nella media del topic.

Logica per ogni metrica
-----------------------
  1. Cerca il valore del giocatore: DB (fonte appropriata) -> Excel fallback.
  2. Se distribuzione disponibile: percentile rank vs 200 valori (kind='mean').
  3. Altrimenti: score ATP-relativo continuo [0,100], ATP avg = 50.
  4. Metriche parity=-1 invertite: 100 - score.
  5. Metriche parity=0 escluse.

Coefficiente topic = media pesata (peso 2 per top200, peso 1 per atp_rel).
Coefficiente Global = media aritmetica dei 6 topic.

Uso
---
    python aggressiveness_v2_top200.py tables_sonego_bellucci.xlsx
    python aggressiveness_v2_top200.py file.xlsx --db aggressiveness_coeff.db
                                                 --general-db ../tennis_abstract_merged.db
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import percentileofscore


# =========================================================================
# Topics + parita
# Mappa: nome metrica (uguale all'indicatore nella tabella "full" dell'Excel)
#        -> (colonna DB top200 o None, parita: +1 / -1 / 0)
#
# +1  = piu alto e meglio
# -1  = piu basso e meglio (il percentile viene invertito: 100 - p)
#  0  = neutro, escluso dal calcolo
#
# Se la colonna DB e' None il percentile viene calcolato sul campione
# a 3 punti [ATP avg, p1, p2] letti dall'Excel (fallback).
# =========================================================================
TOPICS: dict[str, dict[str, tuple[str | None, int]]] = {
    "Serve": {
        "1stIn":                          ("Serve | 1stIn", +1),
        "1st won %":                      ("Serve | 1st%",  +1),
        "2nd won %":                      ("Serve | 2nd%",  +1),
        #"Aces %":                         ("Serve | A%",    +1),
        "Double Faults %":                ("Serve | DF%",   -1),
        #"Serve Points won":               ("Serve | SPW",   +1),
        #"Serve Games hold %":             ("Serve | Hld%",  +1),
        "1st Speed":                      (None,            +1),
        "1st T Speed":                    (None,            +1),
        "1st Wide Speed":                 (None,            +1),
        "2nd Speed":                      (None,            +1),
        "2nd T Speed":                    (None,            +1),
        "2nd Wide Speed":                 (None,            +1),
        #"Break Pts Saved":                (None,            +1),
        #"Break Pts Faced  Games %":       (None,            -1),
        #"Games won with Break Pts Faced": (None,            +1),
        "Deuce Aces %":                   (None,            +1),
        "Ad Aces %":                      (None,            +1),
        #"Deuce Service Pts won %":        (None,            +1),
        "Ad Service Pts won %":           (None,            +1),
        #"Serve & Volley won%":            (None,            +1),
        #"Service Impact":                 (None,            +1),
        #"Serve Impact 1st":               (None,            +1),
    },
    "Return": {
        "Break %":                        ("gen:Break %",               +1),
        "Return Points won":              ("Efficiency | RPW",          +1),
        "Return in Play won %":           (None,                       +1),
        "Break Pts converted":            ("Attitude | BP_Conv",        +1),
        #"Break vs Games with Break Pts":  ("Efficiency | BP_Conv/BPG",  +1),
        #"Games with Break Pts":           ("Efficiency | BP_Games",     +1),
        "Deuce Return Pts won%":          (None,                       +1),
        "Ad Return Pts won %":            (None,                       +1),
        "Return shallow %":               ("gen:Shallow",              -1),
        "Return deep %":                  ("gen:Deep",                 +1),
        "Return very deep %":             ("gen:Very Deep",            +1),
        "Return unforced %":              ("gen:Unforced",             -1),
    },
    "Rally": {
        "Forehand Wnr Pts":               ("Rally | FH_Wnr/Pt",  +1),
        "Backhand Wnr Pts":               ("Rally | BH_Wnr/Pt",  +1),
        "Winners":                        ("Efficiency | Wnr/Pt", +1),
        "Unforced Errors":                ("Rally | UFE/Pt",      -1),
        "Inside Out Winners":             (None,                  +1),
        "Down the Line Winners":          (None,                  +1),
        "Down the Line Winners 1st":      (None,                  +1),
        "Ratio Winners / Unforced":       ("Rally | Ratio",       +1),
    },
    "Attitude": {
        "Break Back%":                    ("Attitude | BreakBack%",  +1),
        "Return Aggressive":              ("Attitude | ReturnAgg",   +1),
        "Rally Aggressive":               ("Attitude | RallyAgg",    +1),
        "Serve StayMatch won":            ("Attitude | SvStayMatch", +1),
        "Serve ForMatch won":             (None,                     +1),
        "Break Consolidation %":          ("Attitude | Consol%",     +1),
        "Tie Break won":                  ("Attitude | TB%",         +1),
        "Dominance Ratio":                ("gen:Dominance Ratio",     +1),
    },
    "Tactics": {
        "Drop Frequency":                 ("Tactics | Drop:_Freq",    0),
        "Net Frequency":                  ("Tactics | Net_Freq",     +1),
        "Serve & Volley  Frequency":      ("Tactics | SnV_Freq",     +1),
        "Cross court %":                  ("Tactics | crosscourt",    0),
        "Down middle %":                  (None,                      0),
        "Down the Line %":                ("Tactics | down_the_line", +1),
        "Inside Out %":                   ("Tactics | inside_out",   +1),
        "Inside In%":                     ("Tactics | inside_in",    +1),
    },
    "Efficiency": {
        "Serve Points won":               ("Efficiency | SPW",         +1),
        "Serve Games hold %":             ("Efficiency | Hld%",        +1),
        "Return Points won":              ("Efficiency | RPW",         +1),
        "Break Pts Faced  Games %":       (None,                       -1),
        "Serve & Volley won%":            (None,                       +1),
        "Break vs Games with Break Pts":  ("Efficiency | BP_Conv/BPG", +1),
        "Games with Break Pts":           ("Efficiency | BP_Games",    +1),
        "Winners":                        ("Efficiency | Wnr/Pt",      +1),
        "Unforced Errors":                ("Rally | UFE/Pt",           -1),
        "Ratio Winners / Unforced":       ("Efficiency | Ratio",       +1),
    },
}


# =========================================================================
# Parser numerico
# =========================================================================
PERCENT_RE = re.compile(r"(-?\d+(?:[.,]\d+)?)\s*%")
NUMBER_RE  = re.compile(r"-?\d+(?:[.,]\d+)?")


def parse_numeric(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        return float(value)
    text = str(value).replace("\xa0", " ").strip()
    if text in ("", "NA", "-", "None", "NULL", "nan"):
        return None
    m = PERCENT_RE.search(text)
    if m:
        return float(m.group(1).replace(",", "."))
    m = NUMBER_RE.search(text)
    if m:
        return float(m.group(0).replace(",", "."))
    return None


def normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value)).strip().lower()
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[\s_\-]+", "", re.sub(r"[^a-z0-9\s]", "", text))


# =========================================================================
# Lettura file
# =========================================================================
def parse_player_filename(xlsx_path: Path) -> tuple[str, str]:
    stem = xlsx_path.stem.lower()
    stem = re.sub(r"^tables_", "", stem)
    stem = re.sub(r"_+\d+_*$", "", stem)
    parts = [p for p in stem.split("_") if p]
    if len(parts) >= 2:
        return parts[0].capitalize(), parts[1].capitalize()
    return "Player1", "Player2"


def load_excel_player_values(xlsx_path: Path) -> dict[str, dict[str, dict]]:
    """Restituisce {topic: {metrica: {atp, p1, p2}}} leggendo la tabella 'full' (cols 5-8)."""
    out: dict[str, dict[str, dict]] = {}
    excel = pd.ExcelFile(xlsx_path)
    for sheet in excel.sheet_names:
        df = excel.parse(sheet)
        # La tabella "full" e' sempre alle colonne 5-8:
        # col5=indicator, col6=ATP avg, col7=p1, col8=p2
        full = df.iloc[:, 5:9].copy()
        full.columns = ["indicator", "atp", "p1", "p2"]
        full = full.dropna(subset=["indicator"]).reset_index(drop=True)
        topic_data: dict[str, dict] = {}
        for _, row in full.iterrows():
            ind = str(row["indicator"]).strip()
            if ind in ("indicator", "nan", "NaN"):
                continue
            topic_data[ind] = {
                "atp": parse_numeric(row["atp"]),
                "p1":  parse_numeric(row["p1"]),
                "p2":  parse_numeric(row["p2"]),
            }
        out[sheet] = topic_data
    return out


GEN_MIN_NON_NULL = 100  # soglia minima valori non-null per usare general come distribuzione


def load_top200_distributions(
    db_path: Path,
    general_db_path: Path | None = None,
) -> tuple[dict[str, list[float]], pd.DataFrame, pd.DataFrame]:
    """Carica distribuzioni da aggressiveness_coeff.db e da general (tennis_abstract_merged.db).

    Le colonne di general vengono prefissate con "gen:" nel dizionario distrib.
    Solo colonne con >= GEN_MIN_NON_NULL valori non-null vengono incluse da general.

    Restituisce (distrib, df_agg, df_gen).
    """
    if not db_path.exists():
        raise FileNotFoundError(f"DB top200 non trovato: {db_path}")

    # --- aggressiveness_coeff.db ---
    conn = sqlite3.connect(db_path)
    df_agg = pd.read_sql_query("SELECT * FROM glossary_top200", conn)
    conn.close()

    distrib: dict[str, list[float]] = {}
    for col in df_agg.columns:
        if col in ("ranking_order", "player_key", "player_name"):
            continue
        vals = pd.to_numeric(df_agg[col], errors="coerce").dropna().tolist()
        distrib[col] = vals

    # --- tennis_abstract_merged.db / general ---
    df_gen = pd.DataFrame()
    if general_db_path and general_db_path.exists():
        conn = sqlite3.connect(general_db_path)
        df_gen = pd.read_sql_query("SELECT * FROM general", conn)
        conn.close()

        for col in df_gen.columns:
            if col == "player_name":
                continue
            parsed = df_gen[col].apply(parse_numeric)
            non_null_count = parsed.notna().sum()
            if non_null_count >= GEN_MIN_NON_NULL:
                distrib[f"gen:{col}"] = parsed.dropna().tolist()

    return distrib, df_agg, df_gen


def lookup_player_row(df: pd.DataFrame, target_name: str) -> pd.Series | None:
    """Cerca un giocatore nel DB con matching tollerante."""
    norm_target = normalize_name(target_name)
    for col in ("player_key", "player_name"):
        if col not in df.columns:
            continue
        for _, row in df.iterrows():
            if normalize_name(row[col]).startswith(norm_target) or \
               normalize_name(row[col]).endswith(norm_target) or \
               norm_target in normalize_name(row[col]):
                return row
    return None


# =========================================================================
# Calcolo coefficienti
# =========================================================================
WEIGHT_DB    = 2.0   # metriche con distribuzione top200 (campione 200 giocatori)
WEIGHT_EXCEL = 1.0   # metriche senza DB, score relativo all'ATP avg


def _atp_relative_score(raw_val: float, atp_val: float, parity: int) -> float:
    """Punteggio lineare relativo all'ATP avg, scala [0, 100].

    ATP avg = 50 per definizione.
    parity=+1: score = (player / atp) * 50   (sopra media > 50)
    parity=-1: score = (atp / player) * 50   (meno e' meglio, invertito)
    """
    if atp_val == 0:
        return 50.0
    if parity == 1:
        score = (raw_val / atp_val) * 50.0
    else:
        score = (atp_val / raw_val) * 50.0 if raw_val != 0 else 100.0
    return min(100.0, max(0.0, score))


def compute_player_topic(
    topic_name: str,
    player_name: str,
    excel_data: dict[str, dict[str, dict]],
    distrib: dict[str, list[float]],
    db_df_agg: pd.DataFrame,
    db_df_gen: pd.DataFrame,
    who: str,                    # 'p1' o 'p2'
) -> tuple[float, dict]:
    """Coeff topic + dettaglio per un giocatore.

    Sorgenti dei valori raw:
      "agg:" db_df_agg (aggressiveness_coeff.db)
      "gen:" db_df_gen (tennis_abstract_merged.db/general)  -> no fallback Excel
      None   Excel fallback -> score ATP-relativo

    Pesi: top200 (agg o gen) = WEIGHT_DB, atp_rel = WEIGHT_EXCEL.
    """
    topic = TOPICS[topic_name]
    player_row_agg = lookup_player_row(db_df_agg, player_name)
    player_row_gen = (lookup_player_row(db_df_gen, player_name)
                      if not db_df_gen.empty else None)

    weighted_sum = 0.0
    weight_total = 0.0
    detail: dict[str, dict] = {}

    for metric, (db_col, parity) in topic.items():
        if parity == 0:
            continue

        is_gen = bool(db_col and db_col.startswith("gen:"))
        actual_col = db_col[4:] if is_gen else db_col

        # --- valore grezzo del giocatore ---
        raw_val: float | None = None
        source = "Excel"

        if is_gen:
            # Solo general table, nessun fallback Excel (scale diverse)
            if player_row_gen is not None and actual_col:
                raw_val = parse_numeric(player_row_gen.get(actual_col))
                if raw_val is not None:
                    source = "gen"
        elif actual_col:
            # aggressiveness_coeff.db, poi Excel
            if player_row_agg is not None:
                db_val = player_row_agg.get(actual_col)
                if db_val is not None and not (isinstance(db_val, float) and pd.isna(db_val)):
                    raw_val = float(db_val)
                    source = "agg"
            if raw_val is None:
                for sheet_dict in excel_data.values():
                    entry = sheet_dict.get(metric)
                    if entry and entry.get(who) is not None:
                        raw_val = entry[who]
                        break
        else:
            # Nessun DB: solo Excel
            for sheet_dict in excel_data.values():
                entry = sheet_dict.get(metric)
                if entry and entry.get(who) is not None:
                    raw_val = entry[who]
                    break

        if raw_val is None:
            continue

        # --- punteggio e peso ---
        has_distrib = bool(db_col and db_col in distrib and len(distrib[db_col]) >= 10)

        if has_distrib:
            p = percentileofscore(distrib[db_col], raw_val, kind="mean")
            if parity == -1:
                p = 100.0 - p
            weight = WEIGHT_DB
            method = "top200"
        else:
            atp_val: float | None = None
            for sheet_dict in excel_data.values():
                entry = sheet_dict.get(metric)
                if entry and entry.get("atp") is not None:
                    atp_val = entry["atp"]
                    break
            if atp_val is None or atp_val == 0:
                continue
            p = _atp_relative_score(raw_val, atp_val, parity)
            weight = WEIGHT_EXCEL
            method = "atp_rel"

        weighted_sum += p * weight
        weight_total += weight
        detail[metric] = {
            "raw":    round(raw_val, 4),
            "pct":    round(p, 1),
            "parity": parity,
            "source": source,
            "method": method,
            "weight": weight,
        }

    coeff = round(weighted_sum / weight_total, 1) if weight_total > 0 else 50.0
    return coeff, detail


def compute_all(
    xlsx_path: Path,
    db_path: Path,
    general_db_path: Path | None = None,
    p1_override: str | None = None,
    p2_override: str | None = None,
) -> dict:
    p1_name, p2_name = parse_player_filename(xlsx_path)
    if p1_override: p1_name = p1_override
    if p2_override: p2_name = p2_override

    # default: cerca tennis_abstract_merged.db nella cartella padre
    if general_db_path is None:
        candidate = xlsx_path.parent.parent / "tennis_abstract_merged.db"
        general_db_path = candidate if candidate.exists() else None

    excel_data = load_excel_player_values(xlsx_path)
    distrib, db_df_agg, db_df_gen = load_top200_distributions(db_path, general_db_path)

    coeffs:  dict[str, dict[str, float]] = {}
    details: dict[str, dict] = {}

    for topic in TOPICS:
        c1, d1 = compute_player_topic(topic, p1_name, excel_data, distrib, db_df_agg, db_df_gen, "p1")
        c2, d2 = compute_player_topic(topic, p2_name, excel_data, distrib, db_df_agg, db_df_gen, "p2")
        coeffs[topic] = {"p1": c1, "p2": c2}
        details[topic] = {"p1": d1, "p2": d2}

    main_topics = ["Serve", "Return", "Rally", "Attitude", "Tactics", "Efficiency"]
    coeffs["Global"] = {
        "p1": round(float(np.mean([coeffs[t]["p1"] for t in main_topics])), 1),
        "p2": round(float(np.mean([coeffs[t]["p2"] for t in main_topics])), 1),
    }

    gen_loaded = not db_df_gen.empty
    return {
        "p1_name":    p1_name,
        "p2_name":    p2_name,
        "coeffs":     coeffs,
        "metrics":    details,
        "sample_size": max(len(v) for v in distrib.values()),
        "general_db": gen_loaded,
    }


# =========================================================================
# Output
# =========================================================================
def print_results(result: dict) -> None:
    p1, p2 = result["p1_name"], result["p2_name"]
    print()
    print("=" * 60)
    print(f"  Aggressiveness Coefficient v2 — {p1} vs {p2}")
    print(f"  Campione di riferimento: top {result['sample_size']} ATP")
    print("=" * 60)
    print(f"{'Topic':<13} {p1:>10} {p2:>10}   Vincitore")
    print("-" * 60)
    for topic in ["Serve","Return","Rally","Attitude","Tactics","Efficiency","Global"]:
        if topic not in result["coeffs"]:
            continue
        v1, v2 = result["coeffs"][topic]["p1"], result["coeffs"][topic]["p2"]
        if abs(v1 - v2) < 0.05:
            winner = "—"
        else:
            winner = p1 if v1 > v2 else p2
        marker = "*" if topic == "Global" else " "
        print(f"{marker} {topic:<11} {v1:>10.1f} {v2:>10.1f}   {winner}")
    print("-" * 60)


def save_outputs(result: dict, xlsx_path: Path, output_dir: Path | None = None) -> None:
    base_dir = output_dir or xlsx_path.parent
    p1, p2  = result["p1_name"].lower(), result["p2_name"].lower()

    coeffs_path = base_dir / f"coeffs_{p1}_{p2}.json"
    coeffs_path.write_text(json.dumps(result["coeffs"], indent=2, ensure_ascii=False))
    print(f"\n[OK] Coefficienti salvati: {coeffs_path}")

    metrics_path = base_dir / f"pcts_{p1}_{p2}.json"
    metrics_path.write_text(json.dumps(result["metrics"], indent=2, ensure_ascii=False))
    print(f"[OK] Percentili per metrica: {metrics_path}")


# =========================================================================
# CLI
# =========================================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Calcola i coefficienti di aggressivita v2 vs top 200 ATP.",
    )
    parser.add_argument("xlsx", type=Path, help="Percorso al file Excel match")
    parser.add_argument("--db", type=Path,
                        default=Path("aggressiveness_coeff.db"),
                        help="Path al DB glossary_top200 (default: aggressiveness_coeff.db)")
    parser.add_argument("--general-db", type=Path, default=None,
                        help="Path a tennis_abstract_merged.db (default: ../tennis_abstract_merged.db)")
    parser.add_argument("-o", "--output-dir", type=Path, default=None)
    parser.add_argument("--p1", type=str, default=None,
                        help="Nome esplicito player 1 (override del filename)")
    parser.add_argument("--p2", type=str, default=None,
                        help="Nome esplicito player 2 (override del filename)")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if not args.xlsx.exists():
        print(f"Errore: file non trovato: {args.xlsx}", file=sys.stderr)
        sys.exit(1)

    result = compute_all(args.xlsx, args.db, args.general_db, args.p1, args.p2)
    if not args.quiet:
        print_results(result)
    save_outputs(result, args.xlsx, args.output_dir)


if __name__ == "__main__":
    main()
