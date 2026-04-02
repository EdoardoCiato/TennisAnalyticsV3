from __future__ import annotations

import re
import sqlite3
import sys
import unicodedata
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
TOP200_PATH = PROJECT_ROOT / "top200.txt"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from report_layout import REPORT_SPECS
from specific_tables_chat import DB_PATH, apply_value_scale, source_numeric_value, table_exists

OUTPUT_DB_PATH = BASE_DIR / "aggressiveness_coeff.db"
OUTPUT_TABLE_NAME = "glossary_top200"

MCP_TABLE_BY_STAT = {
    "crosscourt": "mcp_m_stats_shotdirection_enriched",
    "down_the_line": "mcp_m_stats_shotdirection_enriched",
    "inside_in": "mcp_m_stats_shotdirection_enriched",
    "inside_out": "mcp_m_stats_shotdirection_enriched",
}


def sql_quote(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def clean_category_name(sheet_name: str) -> str:
    return sheet_name.replace(" Analysis", "")


def output_column_name(category: str, code: str) -> str:
    return f"{category} | {code}"


def ordered_metric_specs() -> list[tuple[str, dict]]:
    ordered_fields: list[tuple[str, dict]] = []
    for spec in REPORT_SPECS:
        category = clean_category_name(spec["sheet"])
        for field in spec["fields"]:
            ordered_fields.append((category, field))
    return ordered_fields


def required_source_tables() -> set[str]:
    tables = set()
    for _, field in ordered_metric_specs():
        source = field["source"]
        if source.get("kind") == "mcp_comparison":
            table_name = MCP_TABLE_BY_STAT.get(source["stat"])
            if table_name:
                tables.add(table_name)
            continue
        table = source.get("table")
        if table:
            tables.add(table)
    return tables


def load_top200_players() -> list[str]:
    players = []
    for line in TOP200_PATH.read_text(encoding="utf-8").splitlines():
        player = line.strip()
        if player:
            players.append(player)
    return players


def pretty_player_name(player_key: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", " ", player_key).strip()


def normalize_name(value: str) -> str:
    if not value:
        return ""
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[\s_\-]+", "", text)
    text = re.sub(r"[^a-z0-9]", "", text)
    return text


def table_columns(conn: sqlite3.Connection, table_name: str, cache: dict[str, set[str]]) -> set[str]:
    if table_name not in cache:
        cache[table_name] = {
            row[1] for row in conn.execute(f"PRAGMA table_info({sql_quote(table_name)})").fetchall()
        }
    return cache[table_name]


def build_mcp_name_map(
    conn: sqlite3.Connection,
    table_names: set[str],
    columns_cache: dict[str, set[str]],
) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for table_name in sorted(table_names):
        if not table_exists(conn.cursor(), table_name):
            continue
        columns = table_columns(conn, table_name, columns_cache)
        for candidate in ("player_name_mcp", "player", "player_name_dim"):
            if candidate not in columns:
                continue
            rows = conn.execute(
                f"SELECT DISTINCT {sql_quote(candidate)} FROM {sql_quote(table_name)} "
                f"WHERE {sql_quote(candidate)} IS NOT NULL"
            ).fetchall()
            for (value,) in rows:
                if value:
                    mapping[normalize_name(value)] = value
    return mapping


def fetch_mcp_numeric(
    conn: sqlite3.Connection,
    player_key: str,
    stat_key: str,
    columns_cache: dict[str, set[str]],
    mcp_name_map: dict[str, str],
) -> float | None:
    table_name = MCP_TABLE_BY_STAT[stat_key]
    real_name = mcp_name_map.get(normalize_name(player_key))
    if not real_name:
        return None

    columns = table_columns(conn, table_name, columns_cache)
    if stat_key not in columns:
        return None

    name_clauses = []
    params = []
    for candidate in ("player_name_mcp", "player", "player_name_dim"):
        if candidate in columns:
            name_clauses.append(f"{sql_quote(candidate)} = ?")
            params.append(real_name)

    if not name_clauses:
        return None

    row = conn.execute(
        f"SELECT AVG(CAST(NULLIF(TRIM({sql_quote(stat_key)}), '') AS REAL)) "
        f"FROM {sql_quote(table_name)} "
        f"WHERE ({' OR '.join(name_clauses)}) "
        f"AND {sql_quote(stat_key)} IS NOT NULL "
        f"AND TRIM({sql_quote(stat_key)}) NOT IN ('', 'NA')",
        params,
    ).fetchone()
    value = row[0] if row else None
    return round(value, 4) if value is not None else None


def source_numeric_for_top200(
    conn: sqlite3.Connection,
    source: dict,
    player_key: str,
    columns_cache: dict[str, set[str]],
    mcp_name_map: dict[str, str],
) -> float | None:
    if source["kind"] == "mcp_comparison":
        return fetch_mcp_numeric(conn, player_key, source["stat"], columns_cache, mcp_name_map)
    return source_numeric_value(conn, source, player_key, player_key)


def build_top200_glossary_dataframe(conn: sqlite3.Connection) -> pd.DataFrame:
    players = load_top200_players()
    columns_cache: dict[str, set[str]] = {}
    mcp_name_map = build_mcp_name_map(conn, set(MCP_TABLE_BY_STAT.values()), columns_cache)

    rows: list[dict] = []
    for ranking_order, player_key in enumerate(players, start=1):
        row = {
            "ranking_order": ranking_order,
            "player_key": player_key,
            "player_name": pretty_player_name(player_key),
        }

        for category, field in ordered_metric_specs():
            source = field["source"]
            numeric_value = source_numeric_for_top200(
                conn,
                source,
                player_key,
                columns_cache,
                mcp_name_map,
            )
            scaled_value = apply_value_scale(conn, numeric_value, source)
            row[output_column_name(category, field["code"])] = (
                round(float(scaled_value), 4) if scaled_value is not None else None
            )

        rows.append(row)

    ordered_columns = ["ranking_order", "player_key", "player_name"]
    ordered_columns += [
        output_column_name(category, field["code"]) for category, field in ordered_metric_specs()
    ]

    return pd.DataFrame(rows)[ordered_columns]


def export_sqlite(df: pd.DataFrame) -> None:
    if OUTPUT_DB_PATH.exists():
        OUTPUT_DB_PATH.unlink()

    with sqlite3.connect(OUTPUT_DB_PATH) as conn:
        df.to_sql(OUTPUT_TABLE_NAME, conn, if_exists="replace", index=False)
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{OUTPUT_TABLE_NAME}_ranking_order" '
            f'ON "{OUTPUT_TABLE_NAME}" ("ranking_order")'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{OUTPUT_TABLE_NAME}_player_key" '
            f'ON "{OUTPUT_TABLE_NAME}" ("player_key")'
        )
        conn.commit()


def main() -> None:
    if not TOP200_PATH.exists():
        raise FileNotFoundError(f"File top 200 non trovato: {TOP200_PATH}")

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        missing_tables = [name for name in sorted(required_source_tables()) if not table_exists(cursor, name)]
        if missing_tables:
            raise RuntimeError(
                "Tabelle mancanti per la tabella glossary top 200: " + ", ".join(missing_tables)
            )

        final_df = build_top200_glossary_dataframe(conn)

    export_sqlite(final_df)

    print(f"Creato database: {OUTPUT_DB_PATH}")
    print(f"Tabella creata: {OUTPUT_TABLE_NAME}")
    print(f"Righe create: {final_df.shape[0]}")
    print(f"Colonne create: {final_df.shape[1]}")
    print("Ordinamento applicato secondo scrapers/top200.txt")


if __name__ == "__main__":
    main()
