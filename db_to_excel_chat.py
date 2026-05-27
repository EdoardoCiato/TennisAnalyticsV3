"""
Export Excel — Tennis Analytics V3
==================================

Genera tennis_abstract_output.xlsx con 3 fogli:

  * Indices Top200    indici per topic (Serve, Return, Rally, Attitude,
                      Tactics, Efficiency, Global) — percentile vs top 200
  * Indices Glossary  metriche grezze sorgenti degli indici
  * Matches Analyzed  totale partite analizzate per giocatore (MCP)

Sorgenti dati:
  - aggressiveness_indices.db        (build_aggressiveness_indices.py)
  - tennis_abstract_new_version.db   (count_matches_per_player.py)

Se l'utente ha analizzato un sottoinsieme di top 200, anche il foglio
"Matches Analyzed" viene filtrato a quei giocatori per restare coerente.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

# =========================
# CONFIG
# =========================
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
RAW_DB_PATH     = os.path.join(BASE_DIR, "tennis_abstract_new_version.db")
INDICES_DB_PATH = os.path.join(BASE_DIR, "aggressiveness_indices.db")
XLSX_PATH       = os.path.join(BASE_DIR, "tennis_abstract_output.xlsx")

# Polarity per il coloring (verde = max, arancio = min, parity +1/-1)
INDICES_POLARITY: dict[str, int] = {
    "Serve":      +1,
    "Return":     +1,
    "Rally":      +1,
    "Attitude":   +1,
    "Tactics":    +1,
    "Efficiency": +1,
    "Global":     +1,
}

MATCHES_POLARITY: dict[str, int] = {
    "Matches Analyzed": +1,
}

# =========================
# STYLES
# =========================
FILL_GREEN   = PatternFill(start_color="00C853", end_color="00C853", fill_type="solid")
FILL_ORANGE  = PatternFill(start_color="FF9800", end_color="FF9800", fill_type="solid")
FILL_HEADER  = PatternFill(start_color="2C5F8A", end_color="2C5F8A", fill_type="solid")
THIN         = Side(border_style="thin", color="CCCCCC")
BORDER       = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)


# =========================
# Helpers
# =========================
def safe_sheet_name(name: str, used: set[str]) -> str:
    base, candidate, n = name[:31], name[:31], 1
    while candidate in used:
        suffix    = f"_{n}"
        candidate = base[:31 - len(suffix)] + suffix
        n        += 1
    used.add(candidate)
    return candidate


def to_float(value) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text in ("", "NA", "-", "None", "NULL", "nan"):
        return None
    try:
        return float(text.replace("%", "").replace(",", "."))
    except Exception:
        return None


# =========================
# Sheet styling
# =========================
def apply_header_style(ws, ncols: int) -> None:
    for col in range(1, ncols + 1):
        cell           = ws.cell(row=1, column=col)
        cell.fill      = FILL_HEADER
        cell.font      = Font(bold=True, color="FFFFFF")
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = BORDER
    ws.row_dimensions[1].height = 36
    ws.freeze_panes             = "A2"
    ws.auto_filter.ref          = ws.dimensions


def apply_full_borders(ws, df: pd.DataFrame) -> None:
    for row in range(2, df.shape[0] + 2):
        for col in range(1, df.shape[1] + 1):
            cell           = ws.cell(row=row, column=col)
            cell.border    = BORDER
            cell.alignment = Alignment(horizontal="center", vertical="center")


def apply_coloring(ws, df: pd.DataFrame, polarity_map: dict[str, int]) -> None:
    for col_idx, col_name in enumerate(df.columns, start=1):
        polarity = polarity_map.get(str(col_name))
        if polarity not in (+1, -1):
            continue
        nums  = [to_float(df.iloc[i][col_name]) for i in range(df.shape[0])]
        valid = [v for v in nums if v is not None]
        if len(valid) < 2:
            continue
        mn, mx = min(valid), max(valid)
        for row_idx, v in enumerate(nums, start=2):
            if v is None:
                continue
            cell = ws.cell(row=row_idx, column=col_idx)
            if polarity == +1:
                if v == mx:   cell.fill = FILL_GREEN
                elif v == mn: cell.fill = FILL_ORANGE
            else:
                if v == mn:   cell.fill = FILL_GREEN
                elif v == mx: cell.fill = FILL_ORANGE


def set_column_widths(ws, df: pd.DataFrame) -> None:
    for col_idx, col_name in enumerate(df.columns, start=1):
        max_len = len(str(col_name))
        for i in range(df.shape[0]):
            v = df.iloc[i, col_idx - 1]
            if v is not None:
                max_len = max(max_len, len(str(v)))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max(max_len + 2, 12), 30)


def write_sheet(
    writer,
    df: pd.DataFrame,
    sheet_name: str,
    used_sheets: set[str],
    polarity_map: dict[str, int] | None = None,
) -> None:
    name = safe_sheet_name(sheet_name, used_sheets)
    df.to_excel(writer, sheet_name=name, index=False)
    ws = writer.book[name]
    apply_header_style(ws, df.shape[1])
    apply_full_borders(ws, df)
    if polarity_map:
        apply_coloring(ws, df, polarity_map)
    set_column_widths(ws, df)
    print(f"   OK '{name}' ({df.shape[0]}x{df.shape[1]})")


# =========================
# Main export
# =========================
def _resolve_writable_path(xlsx_path: str) -> str:
    """Se xlsx_path e' bloccato (Excel aperto su Windows), ritorna un path
    affiancato con timestamp; altrimenti ritorna xlsx_path invariato.
    """
    if not os.path.exists(xlsx_path):
        return xlsx_path
    try:
        # Test di scrittura rapido: prova ad aprire in append+binary
        with open(xlsx_path, "ab"):
            pass
        return xlsx_path
    except PermissionError:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        p = Path(xlsx_path)
        alt = str(p.with_name(f"{p.stem}_{ts}{p.suffix}"))
        print(f"\n  WARN: '{xlsx_path}' e' bloccato (Excel aperto?).")
        print(f"        Scrivo invece in: {alt}")
        print(f"        Chiudi Excel se vuoi sovrascrivere il file principale.")
        return alt


def export_to_excel(
    xlsx_path: str = XLSX_PATH,
    indices_db_path: str = INDICES_DB_PATH,
    raw_db_path: str = RAW_DB_PATH,
) -> None:
    print(f"  Indices DB : {indices_db_path}")
    print(f"  Raw DB     : {raw_db_path}")
    print(f"  Output     : {xlsx_path}")

    if not os.path.exists(indices_db_path):
        raise FileNotFoundError(
            f"{indices_db_path} non trovato. "
            "Esegui prima: python build_aggressiveness_indices.py"
        )

    xlsx_path = _resolve_writable_path(xlsx_path)
    used_sheets: set[str] = set()

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:

        # ---- 1) Indices Top200 + 2) Indices Glossary ---------------------
        conn = sqlite3.connect(indices_db_path)
        try:
            df_idx = pd.read_sql_query('SELECT * FROM "indices_top200"', conn)
            df_idx = df_idx.sort_values("Global", ascending=False, na_position="last")
            selected_keys = set(df_idx["player_key"].tolist())
            print(f"\n  Indici per {len(df_idx)} giocatori")
            write_sheet(writer, df_idx, "Indices Top200",
                        used_sheets, INDICES_POLARITY)

            df_glo = pd.read_sql_query('SELECT * FROM "glossary_top200"', conn)
            write_sheet(writer, df_glo, "Indices Glossary",
                        used_sheets, None)
        finally:
            conn.close()

        # ---- 3) Matches Analyzed (count_matches_per_player) --------------
        if not os.path.exists(raw_db_path):
            print(f"\n  WARN: {raw_db_path} non trovato — skip 'Matches Analyzed'")
        else:
            import count_matches_per_player as cmp_mod
            rows = cmp_mod.collect(Path(raw_db_path))
            df_matches = (
                pd.DataFrame(rows)
                  .drop(columns=["table"], errors="ignore")
                  .rename(columns={
                      "player":           "Player",
                      "player_key":       "Player Key",
                      "matches_analyzed": "Matches Analyzed",
                      "source":           "Source",
                  })
            )
            df_matches = df_matches[df_matches["Player Key"].isin(selected_keys)]
            print(f"\n  Matches Analyzed: {len(df_matches)} giocatori "
                  f"(filtro = giocatori in indices_top200)")
            write_sheet(writer, df_matches, "Matches Analyzed",
                        used_sheets, MATCHES_POLARITY)

    print(f"\nOK Excel creato: {xlsx_path}  ({len(used_sheets)} fogli)")


if __name__ == "__main__":
    export_to_excel()