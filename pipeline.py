from __future__ import annotations

import os
import sqlite3
import sys
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# DB prodotti da ogni step
DB_RAW    = os.path.join(BASE_DIR, "tennis_abstract_new_version.db")
DB_MERGED = os.path.join(BASE_DIR, "tennis_abstract_new_version_merged.db")
DB_COEFF  = os.path.join(BASE_DIR, "aggressiveness_coeff.db")
DB_AGG_V2 = os.path.join(BASE_DIR, "aggressiveness_v2.db")


def _table_exists(db_path: str, table_name: str) -> bool:
    if not os.path.exists(db_path):
        return False
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
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


def run_pipeline(headless: bool = True) -> None:
    total_start = time.time()

    # ------------------------------------------------------------------
    # STEP 1 — Scraping Tennis Abstract
    # Output: tennis_abstract_new_version.db
    # ------------------------------------------------------------------
    if os.path.exists(DB_RAW):
        _skip("1 / 6  Scraping Tennis Abstract")
    else:
        _step("1 / 6  Scraping Tennis Abstract")
        t = time.time()

        import tennis_scrape_to_sqlite as s1

        top200_path = os.path.join(BASE_DIR, "top200.txt")
        players = []
        with open(top200_path, encoding="utf-8") as f:
            for line in f:
                player = line.strip()
                if player:
                    players.append(player)

        s1.scrape_players_to_sqlite(players, db_path=s1.DB_PATH, headless=headless)

        while s1.missing_players:
            print(f"\n  Riprovo {len(s1.missing_players)} giocatori mancanti...")
            current_missing = s1.missing_players[:]
            s1.missing_players.clear()
            s1.scrape_players_to_sqlite(current_missing, db_path=s1.DB_PATH, headless=headless)

        _done("Scraping Tennis Abstract", time.time() - t)

    # ------------------------------------------------------------------
    # STEP 2 — Aggregazione per schema
    # Output: tennis_abstract_new_version_merged.db
    # ------------------------------------------------------------------
    if os.path.exists(DB_MERGED):
        _skip("2 / 6  Aggregazione tabelle per schema")
    else:
        _step("2 / 6  Aggregazione tabelle per schema")
        t = time.time()

        import tennis_extract_TA_aggr_v3 as s2
        s2.merge_by_structure()

        _done("Aggregazione", time.time() - t)

    # ------------------------------------------------------------------
    # STEP 3 — Enrichment Jeff Sackmann MCP
    # Output: tabelle mcp_* dentro tennis_abstract_new_version_merged.db
    # ------------------------------------------------------------------
    if _table_exists(DB_MERGED, "mcp_m_stats_shotdirection_enriched"):
        _skip("3 / 6  Enrichment Jeff Sackmann MCP")
    else:
        _step("3 / 6  Enrichment Jeff Sackmann MCP")
        t = time.time()

        import tennis_scrape_to_sqlite_JEFFSACKMANN as s3
        s3.main()

        _done("Enrichment MCP", time.time() - t)

    # ------------------------------------------------------------------
    # STEP 4 — Build Aggressiveness Coeff
    # Output: aggressiveness_coeff.db
    # ------------------------------------------------------------------
    if os.path.exists(DB_COEFF):
        _skip("4 / 6  Build Aggressiveness Coefficient")
    else:
        _step("4 / 6  Build Aggressiveness Coefficient")
        t = time.time()

        import build_aggressiveness_coeff as s4
        s4.main()

        _done("Aggressiveness Coeff", time.time() - t)

    # ------------------------------------------------------------------
    # STEP 5 — Aggressiveness Index v2
    # Output: aggressiveness_v2.db
    # ------------------------------------------------------------------
    if os.path.exists(DB_AGG_V2):
        _skip("5 / 6  Aggressiveness Index v2")
    else:
        _step("5 / 6  Aggressiveness Index v2")
        t = time.time()

        import AggV2 as s5
        s5.main()

        _done("Aggressiveness Index v2", time.time() - t)

    # ------------------------------------------------------------------
    # STEP 6 — Export Excel  (eseguito sempre)
    # ------------------------------------------------------------------
    _step("6 / 6  Export Excel")
    t = time.time()

    import db_to_excel_chat as s6
    s6.export_to_excel(s6.DB_PATH, s6.AGG_V2_DB_PATH, s6.XLSX_PATH)

    _done("Export Excel", time.time() - t)

    # ------------------------------------------------------------------
    total = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETATA in {total:.1f}s")
    print(f"  Excel: {s6.XLSX_PATH}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run_pipeline(headless=True)