# tennis_scrape_to_sqlite_JEFFSACKMANN.py
# Posizione: TennisAnalytics-main/ (ROOT)
# Scrive nel STESSO DB di scrapers/tennis_scrape_to_sqlite.py

import os
import re
import time
import sqlite3
import unicodedata
from datetime import datetime, timezone
from typing import Optional, Tuple

import pandas as pd

# =========================
# CONFIG
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # ROOT

# ✅ DB in scrapers/ — stesso di tennis_scrape_to_sqlite.py
DB_PATH = os.path.join(BASE_DIR, "data", "tennis_abstract_new_version_merged.db")

REPO_RAW_BASE = "https://raw.githubusercontent.com/JeffSackmann/tennis_MatchChartingProject/master/"

MCP_FILES = {
    "mcp_m_matches":             "charting-m-matches.csv",
    "mcp_m_stats_shotdirection": "charting-m-stats-ShotDirection.csv",
    "mcp_m_stats_returndepth":   "charting-m-stats-ReturnDepth.csv",
}

PLAYERS_DIM_TABLE    = "players_dim"
MCP_INDEX_TABLE      = "mcp_files_index"
MCP_PLAYER_MAP_TABLE = "mcp_m_player_map"

ENRICHED = {
    "mcp_m_stats_shotdirection": "mcp_m_stats_shotdirection_enriched",
    "mcp_m_stats_returndepth":   "mcp_m_stats_returndepth_enriched",
}

CHUNKSIZE    = 200_000
BATCH_SIZE   = 100
MAX_RETRIES  = 3
BACKOFF_BASE = 5.0


# =========================
# HELPERS
# =========================
def sqlite_insert(table, conn, keys, data_iter):
    data = list(data_iter)
    if not data:
        return
    placeholders = ", ".join(["?"] * len(keys))
    cols = ", ".join([f'"{k}"' for k in keys])
    sql  = f'INSERT INTO "{table.name}" ({cols}) VALUES ({placeholders})'
    conn.execute("BEGIN")
    try:
        for i in range(0, len(data), BATCH_SIZE):
            conn.executemany(sql, data[i:i + BATCH_SIZE])
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def with_retry(fn, *args, label: str = "", **kwargs):
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"   🔄 {label} — tentativo {attempt}/{MAX_RETRIES}")
            result = fn(*args, **kwargs)
            if attempt > 1:
                print(f"   ✅ {label} — riuscito al tentativo {attempt}")
            return result
        except Exception as e:
            last_exc = e
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            print(f"   ❌ {label} — tentativo {attempt} fallito: {e}")
            if attempt < MAX_RETRIES:
                print(f"   ⏳ Attendo {wait:.0f}s...")
                time.sleep(wait)
    raise last_exc


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def norm_name(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip().lower()
    if not s:
        return None
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def ensure_index_table(conn):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {MCP_INDEX_TABLE} (
            table_name TEXT PRIMARY KEY,
            source_file TEXT NOT NULL,
            source_url TEXT NOT NULL,
            rows INTEGER NOT NULL,
            cols INTEGER NOT NULL,
            downloaded_at_utc TEXT NOT NULL
        )
    """)
    conn.commit()


def get_columns(conn, table):
    return [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]


def table_exists(conn, table):
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


# =========================
# DOWNLOAD CSV
# =========================
def _download_chunk(conn, table_name, filename, if_exists, chunksize):
    url    = REPO_RAW_BASE + filename
    print(f"   📥 Downloading: {url}")
    reader = pd.read_csv(url, low_memory=False, chunksize=chunksize)

    total_rows = 0
    cols       = None

    for i, chunk in enumerate(reader, start=1):
        if cols is None:
            cols = list(chunk.columns)
        chunk.to_sql(table_name, conn,
                     if_exists=if_exists if i == 1 else "append",
                     index=False, method=sqlite_insert)
        total_rows += len(chunk)
        print(f"   ✅ Chunk {i}: {total_rows:,} righe")

    ensure_index_table(conn)
    conn.execute(f"""
        INSERT INTO {MCP_INDEX_TABLE}(table_name, source_file, source_url, rows, cols, downloaded_at_utc)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(table_name) DO UPDATE SET
            rows=excluded.rows, cols=excluded.cols, downloaded_at_utc=excluded.downloaded_at_utc
    """, (table_name, filename, url, int(total_rows), int(len(cols or [])), utc_now()))
    conn.commit()
    return total_rows, len(cols or []), url


def load_csv_to_sqlite(conn, table_name, filename, if_exists="replace", chunksize=CHUNKSIZE):
    return with_retry(_download_chunk, conn, table_name, filename, if_exists, chunksize,
                      label=f"download {filename}")


# =========================
# PLAYER MAP
# =========================
def _build_player_map(conn):
    matches_cols = get_columns(conn, "mcp_m_matches")

    def find_col(candidates):
        for c in matches_cols:
            if c.strip().lower().replace(" ", "").replace("_", "") in \
               [x.lower().replace(" ", "").replace("_", "") for x in candidates]:
                return c
        return None

    mid = find_col(["match_id", "matchid"])
    p1  = find_col(["player1", "player 1"])
    p2  = find_col(["player2", "player 2"])

    if not (mid and p1 and p2):
        raise RuntimeError(f"Colonne non trovate: {matches_cols}")

    df = pd.read_sql_query(
        f'SELECT "{mid}" as match_id, "{p1}" as player1, "{p2}" as player2 FROM mcp_m_matches', conn
    )

    map_rows = []
    for _, r in df.iterrows():
        map_rows.append({"match_id": r["match_id"], "player": 1, "player_name_mcp": r["player1"]})
        map_rows.append({"match_id": r["match_id"], "player": 2, "player_name_mcp": r["player2"]})

    m = pd.DataFrame(map_rows)
    m["player_name_norm"] = m["player_name_mcp"].map(norm_name)
    m.to_sql(MCP_PLAYER_MAP_TABLE, conn, if_exists="replace", index=False, method=sqlite_insert)
    conn.execute(f'CREATE INDEX IF NOT EXISTS idx_mcp_map_mid ON {MCP_PLAYER_MAP_TABLE}(match_id, player)')
    conn.execute(f'CREATE INDEX IF NOT EXISTS idx_mcp_map_norm ON {MCP_PLAYER_MAP_TABLE}(player_name_norm)')
    conn.commit()


def build_mcp_player_map(conn):
    with_retry(_build_player_map, conn, label="build_mcp_player_map")


# =========================
# PLAYERS DIM
# =========================
def load_players_dim_norm(conn):
    if not table_exists(conn, PLAYERS_DIM_TABLE):
        print(f"   ⚠️ players_dim non trovata — player_id sarà NULL")
        return pd.DataFrame(columns=["player_id", "player_name", "player_name_norm"])
    cols = get_columns(conn, PLAYERS_DIM_TABLE)
    if not {"player_id", "player_name"}.issubset(set(cols)):
        return pd.DataFrame(columns=["player_id", "player_name", "player_name_norm"])
    dim = pd.read_sql_query(f"SELECT player_id, player_name FROM {PLAYERS_DIM_TABLE}", conn)
    dim["player_name_norm"] = dim["player_name"].map(norm_name)
    return dim.drop_duplicates(subset=["player_name_norm"], keep="first")


# =========================
# ENRICH STATS
# =========================
def _enrich_chunk(conn, src_table, dst_table, mcp_map, dim, mid_col, player_col, offset, first):
    chunk = pd.read_sql_query(
        f'SELECT * FROM "{src_table}" LIMIT {CHUNKSIZE} OFFSET {offset}', conn
    )
    if chunk.empty:
        return 0

    chunk[player_col]    = chunk[player_col].astype(str)
    mcp_map["player"]    = mcp_map["player"].astype(str)
    chunk[mid_col]       = chunk[mid_col].astype(str)
    mcp_map["match_id"]  = mcp_map["match_id"].astype(str)

    chunk = chunk.merge(mcp_map, how="left",
                        left_on=[mid_col, player_col],
                        right_on=["match_id", "player"])

    if not dim.empty:
        chunk = chunk.merge(dim[["player_id", "player_name", "player_name_norm"]],
                            how="left", on="player_name_norm", suffixes=("", "_dim"))
        if "player_name" in chunk.columns:
            chunk.rename(columns={"player_name": "player_name_dim"}, inplace=True)
    else:
        chunk["player_id"]       = None
        chunk["player_name_dim"] = None

    for c in ["match_id_y", "player_y"]:
        if c in chunk.columns:
            chunk.drop(columns=[c], inplace=True)
    if "match_id_x" in chunk.columns:
        chunk.rename(columns={"match_id_x": "match_id"}, inplace=True)
    if "player_x" in chunk.columns:
        chunk.rename(columns={"player_x": "player"}, inplace=True)

    chunk.to_sql(dst_table, conn,
                 if_exists="replace" if first else "append",
                 index=False, method=sqlite_insert)
    return len(chunk)


def enrich_stats_table(conn, src_table, dst_table):
    src_cols   = get_columns(conn, src_table)
    mid_col    = "match_id" if "match_id" in src_cols else None
    player_col = "player"   if "player"   in src_cols else None

    if not (mid_col and player_col):
        raise RuntimeError(f"{src_table}: match_id/player non trovati")

    mcp_map    = pd.read_sql_query(
        f"SELECT match_id, player, player_name_mcp, player_name_norm FROM {MCP_PLAYER_MAP_TABLE}", conn
    )
    dim        = load_players_dim_norm(conn)
    total_rows = conn.execute(f'SELECT COUNT(1) FROM "{src_table}"').fetchone()[0]
    offset     = 0
    first      = True

    while offset < total_rows:
        rows_done = with_retry(_enrich_chunk, conn, src_table, dst_table,
                               mcp_map, dim, mid_col, player_col, offset, first,
                               label=f"enrich {dst_table} offset={offset}")
        if rows_done == 0:
            break
        first   = False
        offset += rows_done
        print(f"   ✅ {dst_table}: {offset:,}/{total_rows:,}")

    try:
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{dst_table}_pid ON "{dst_table}"(player_id)')
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{dst_table}_mid ON "{dst_table}"({mid_col})')
        conn.commit()
    except Exception as e:
        print(f"   ⚠️ Index warning: {e}")


# =========================
# MAIN
# =========================
def main():
    print(f"📂 DB Path: {DB_PATH}")

    if not os.path.exists(DB_PATH):
        print(f"❌ DB non trovato: {DB_PATH}")
        print("   Esegui prima scrapers/tennis_scrape_to_sqlite.py")
        return

    conn = sqlite3.connect(DB_PATH)

    try:
        print("\n=== STEP 1: Download CSV ===")
        for table_name, filename in MCP_FILES.items():
            print(f"\n→ {table_name}")
            rows, cols, _ = load_csv_to_sqlite(conn, table_name, filename, if_exists="replace")
            print(f"   OK: {rows:,} rows, {cols} cols")

        print("\n=== STEP 2: Player map ===")
        build_mcp_player_map(conn)
        print(f"   OK: {MCP_PLAYER_MAP_TABLE}")

        print("\n=== STEP 3: Enrich stats ===")
        for src, dst in ENRICHED.items():
            print(f"\n→ {src} → {dst}")
            enrich_stats_table(conn, src_table=src, dst_table=dst)
            print(f"   OK: {dst}")

        print(f"\n✅ Done. DB: {DB_PATH}")

    except Exception as e:
        print(f"\n❌ Errore: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


if __name__ == "__main__":
    main()