"""
Tennis Analytics V3 — Pipeline principale
=========================================

Orchestrazione end-to-end (5 step + selezione giocatori):

  STEP 0  Selezione interattiva dei giocatori da analizzare
          (default: tutti i 200 di top200.txt)
  STEP 1  Scraping Tennis Abstract     -> tennis_abstract_new_version.db
  STEP 2  Aggregazione per schema      -> tennis_abstract_new_version_merged.db
  STEP 3  Enrichment Jeff Sackmann MCP -> tabelle mcp_* nel DB merged
  STEP 4  Build Aggressiveness Indices -> aggressiveness_indices.db
  STEP 5  Export Excel                 -> tennis_abstract_output.xlsx

Excel finale (3 fogli):
  * Indices Top200    indici per topic (Serve/Return/Rally/Attitude/
                      Tactics/Efficiency/Global), percentile vs top 200
  * Indices Glossary  metriche grezze sorgenti degli indici
  * Matches Analyzed  totale partite analizzate per giocatore (MCP)

Note di design
--------------
- La distribuzione di riferimento per i percentili e' SEMPRE il top 200,
  anche quando l'utente seleziona un sottoinsieme: solo le righe in output
  sono filtrate, mai il campione di confronto.
- Gli step 1-3 lavorano sempre su tutti i 200 (sono prerequisito per la
  distribuzione). Step 4-5 rispettano la selezione utente.
- Tool ad-hoc separato (NON in pipeline): aggressiveness_v2_top200.py
  per analisi head-to-head a partire da un Excel match.
"""

from __future__ import annotations

import os
import re
import sqlite3
import sys
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# DB prodotti da ogni step
DB_RAW     = os.path.join(BASE_DIR, "tennis_abstract_new_version.db")
DB_MERGED  = os.path.join(BASE_DIR, "tennis_abstract_new_version_merged.db")
DB_INDICES = os.path.join(BASE_DIR, "aggressiveness_indices.db")

TOP200_PATH = os.path.join(BASE_DIR, "top200.txt")


# =========================================================================
# Helpers
# =========================================================================
def _table_exists(db_path: str, table_name: str) -> bool:
    if not os.path.exists(db_path):
        return False
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def _step(label: str) -> None:
    print(f"\n{'='*60}")
    print(f"  STEP: {label}")
    print(f"{'='*60}")


def _skip(label: str) -> None:
    print(f"\n{'='*60}")
    print(f"  SKIP: {label}  (output gia' presente)")
    print(f"{'='*60}")


def _done(label: str, elapsed: float) -> None:
    print(f"\n  OK '{label}' completato in {elapsed:.1f}s")


# =========================================================================
# Selezione giocatori
# =========================================================================
def _read_top200_keys(top200_path: str) -> list[str]:
    if not os.path.exists(top200_path):
        return []
    with open(top200_path, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def _norm(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.lower())


def _match_players(query: str, available: list[str]) -> list[str]:
    """Match tollerante (substring case-insensitive normalizzato)."""
    nq = _norm(query)
    if not nq:
        return []
    return [k for k in available if nq in _norm(k)]


def select_players_interactive(top200_path: str) -> list[str] | None:
    """Prompt CLI bloccante: ritorna None per 'tutti' oppure la lista di
    player_key.

    Solo se stdin e' davvero chiuso (EOFError) si fa fallback a 'tutti'.
    """
    available = _read_top200_keys(top200_path)
    print("\n" + "=" * 60)
    print("  STEP 0 / 5  SELEZIONE GIOCATORI DA ANALIZZARE")
    print("=" * 60)
    print(f"  Top 200 disponibili: {len(available)} giocatori")
    print("  Inserisci uno o piu' nomi separati da virgola (anche parziali,")
    print("  es: 'Sinner, Alcaraz, Berret'), oppure premi INVIO per")
    print("  analizzare tutti i 200.")
    print()
    sys.stdout.flush()  # assicura che il prompt sia visibile prima di bloccare

    try:
        raw = input("  > ").strip()
    except EOFError:
        print("  [stdin chiuso] -> analizzo tutti i 200")
        return None

    if not raw or raw.lower() in ("all", "tutti", "*"):
        print("  -> analizzo tutti i 200")
        return None

    selected: list[str] = []
    misses:   list[str] = []
    for name in [n.strip() for n in raw.split(",") if n.strip()]:
        matches = _match_players(name, available)
        if not matches:
            misses.append(name)
        else:
            for m in matches:
                if m not in selected:
                    selected.append(m)

    if misses:
        print(f"  Non trovati: {misses}")
    if not selected:
        print("  Nessun giocatore valido riconosciuto -> analizzo tutti i 200")
        return None

    print(f"\n  Selezionati ({len(selected)}):")
    for k in selected:
        print(f"    - {k}")
    return selected


def ask_refresh_selected(selected_players: list[str] | None) -> bool:
    """Chiede se aggiornare i dati per i giocatori selezionati prima di costruire indici.

    Effetti se True:
      - STEP 1: re-scraping solo dei giocatori selezionati (no full top200)
      - STEP 3: refresh CSV Jeff Sackmann da GitHub
    Mostrato solo se c'e' una selezione (None = full top200 -> non si propone
    di rescrapare 200 giocatori).
    """
    if not selected_players:
        return False
    print()
    print(f"  Vuoi aggiornare i dati per i {len(selected_players)} giocatori selezionati?")
    print(f"  (rescraping pagine Tennis Abstract + refresh CSV MCP da GitHub)")
    print(f"  Tempo stimato: ~{30 * len(selected_players) + 90}s")
    sys.stdout.flush()
    try:
        ans = input("  [s/N] > ").strip().lower()
    except EOFError:
        return False
    yes = ans in ("s", "si", "y", "yes")
    print(f"  -> {'aggiorno i dati' if yes else 'uso i dati in cache'}")
    return yes


# =========================================================================
# Pipeline
# =========================================================================
def run_pipeline(headless: bool = True) -> None:
    total_start = time.time()

    # ------------------------------------------------------------------
    # STEP 0 — Selezione giocatori per indici/Excel
    # ------------------------------------------------------------------
    selected_players = select_players_interactive(TOP200_PATH)
    refresh_selected = ask_refresh_selected(selected_players)

    # ------------------------------------------------------------------
    # STEP 1 — Scraping Tennis Abstract
    # Output: tennis_abstract_new_version.db
    # Modi:
    #   - DB inesistente: full scrape di tutti i 200
    #   - DB esistente + refresh_selected: rescraping SOLO dei selezionati
    #   - DB esistente + no refresh: skip (uso cache)
    # ------------------------------------------------------------------
    if not os.path.exists(DB_RAW):
        _step("1 / 5  Scraping Tennis Abstract (full top200)")
        t = time.time()

        import tennis_scrape_to_sqlite as s1
        players = _read_top200_keys(TOP200_PATH)
        s1.scrape_players_to_sqlite(players, db_path=s1.DB_PATH, headless=headless)


        while s1.missing_players:
            print(f"\n  Riprovo {len(s1.missing_players)} giocatori mancanti...")
            current_missing = s1.missing_players[:]
            s1.missing_players.clear()
            s1.scrape_players_to_sqlite(
                current_missing, db_path=s1.DB_PATH, headless=headless
            )

        _done("Scraping Tennis Abstract (full)", time.time() - t)

    elif refresh_selected and selected_players:
        _step(f"1 / 5  Scraping Tennis Abstract (refresh selezione: "
              f"{len(selected_players)} giocatori)")
        t = time.time()

        import tennis_scrape_to_sqlite as s1
        s1.scrape_players_to_sqlite(
            selected_players, db_path=s1.DB_PATH, headless=headless
        )
        while s1.missing_players:
            print(f"\n  Riprovo {len(s1.missing_players)} giocatori mancanti...")
            current_missing = s1.missing_players[:]
            s1.missing_players.clear()
            s1.scrape_players_to_sqlite(
                current_missing, db_path=s1.DB_PATH, headless=headless
            )

        _done("Scraping Tennis Abstract (refresh)", time.time() - t)
    else:
        _skip("1 / 5  Scraping Tennis Abstract")

    # ------------------------------------------------------------------
    # STEP 2 — Aggregazione per schema
    # Output: tennis_abstract_new_version_merged.db
    # ------------------------------------------------------------------
    if os.path.exists(DB_MERGED):
        _skip("2 / 5  Aggregazione tabelle per schema")
    else:
        _step("2 / 5  Aggregazione tabelle per schema")
        t = time.time()

        import tennis_extract_TA_aggr_v3 as s2
        s2.merge_by_structure()

        _done("Aggregazione", time.time() - t)

    # ------------------------------------------------------------------
    # STEP 3 — Enrichment Jeff Sackmann MCP
    # Output: tabelle mcp_* dentro tennis_abstract_new_version_merged.db
    # Refresh forzato se l'utente ha richiesto l'aggiornamento dati.
    # ------------------------------------------------------------------
    if _table_exists(DB_MERGED, "mcp_m_stats_shotdirection_enriched") and not refresh_selected:
        _skip("3 / 5  Enrichment Jeff Sackmann MCP")
    else:
        label = "3 / 5  Enrichment Jeff Sackmann MCP"
        if refresh_selected:
            label += " (refresh CSV da GitHub)"
        _step(label)
        t = time.time()

        import tennis_scrape_to_sqlite_JEFFSACKMANN as s3
        s3.main()

        _done("Enrichment MCP", time.time() - t)

    # ------------------------------------------------------------------
    # STEP 4 — Build Aggressiveness Indices Top 200
    # Output: aggressiveness_indices.db (glossary_top200 + indices_top200)
    # NB: con selezione utente ricostruisce sempre (cache potrebbe
    # contenere una selezione precedente diversa).
    # ------------------------------------------------------------------
    if selected_players is None and os.path.exists(DB_INDICES):
        _skip("4 / 5  Build Aggressiveness Indices Top200")
    else:
        _step("4 / 5  Build Aggressiveness Indices Top200")
        t = time.time()

        import build_aggressiveness_indices as s4
        s4.build(selected_players=selected_players)

        _done("Aggressiveness Indices Top200", time.time() - t)

    # ------------------------------------------------------------------
    # STEP 5 — Export Excel (eseguito sempre)
    # ------------------------------------------------------------------
    _step("5 / 5  Export Excel")
    t = time.time()

    import db_to_excel_chat as s5
    s5.export_to_excel(s5.XLSX_PATH)

    _done("Export Excel", time.time() - t)

    # ------------------------------------------------------------------
    total = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETATA in {total:.1f}s")
    print(f"  Excel: {s5.XLSX_PATH}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run_pipeline(headless=True)