from __future__ import annotations

import re
import sqlite3
import unicodedata
from collections import defaultdict
from pathlib import Path

import pandas as pd

# =========================
# PATHS
# =========================
BASE_DIR = Path(__file__).resolve().parent
TOP200_PATH = BASE_DIR / "top200.txt"
DB_PATH = str(BASE_DIR / "tennis_abstract_new_version_merged.db")
OUTPUT_DB_PATH = BASE_DIR / "aggressiveness_coeff.db"
OUTPUT_TABLE_NAME = "glossary_top200"

# =========================
# REPORT SPECS
# =========================
CHALLENGER_CAREER_SOURCE = "%Challenger_SeasonsTop"

REPORT_SPECS = [
    {
        "sheet": "Serve Analysis",
        "fields": [
            {"code": "1stIn", "header": "1sts %", "source": {"kind": "group_row", "table": "group_004", "column": "1stIn", "row_column": "Year", "row_value": "Career", "source_like": CHALLENGER_CAREER_SOURCE}},
            {"code": "1st%", "header": "1st won %", "source": {"kind": "group_row", "table": "group_004", "column": "1st%", "row_column": "Year", "row_value": "Career", "source_like": CHALLENGER_CAREER_SOURCE}},
            {"code": "2nd%", "header": "2nd won %", "source": {"kind": "group_row", "table": "group_004", "column": "2nd%", "row_column": "Year", "row_value": "Career", "source_like": CHALLENGER_CAREER_SOURCE}},
            {"code": "A%", "header": "Aces %", "source": {"kind": "group_row", "table": "group_004", "column": "A%", "row_column": "Year", "row_value": "Career", "source_like": CHALLENGER_CAREER_SOURCE}},
            {"code": "Hld%", "header": "Serve Games hold %", "source": {"kind": "group_row", "table": "group_004", "column": "Hld%", "row_column": "Year", "row_value": "Career", "source_like": CHALLENGER_CAREER_SOURCE}},
            {"code": "SPW", "header": "Serve Pts won", "source": {"kind": "group_row", "table": "group_004", "column": "SPW", "row_column": "Year", "row_value": "Career", "source_like": CHALLENGER_CAREER_SOURCE}},
            {"code": "DF%", "header": "Double Faults %", "source": {"kind": "group_row", "table": "group_004", "column": "DF%", "row_column": "Year", "row_value": "Career", "source_like": CHALLENGER_CAREER_SOURCE}},
        ],
    },
    {
        "sheet": "Rally Analysis",
        "fields": [
            {"code": "UFE/Pt", "header": "Unforced Errors", "source": {"kind": "group_average", "table": "group_008", "column": "UFE/Pt"}},
            {"code": "RallyAgg", "header": "Rally Aggressiveness", "source": {"kind": "group_average", "table": "group_016", "column": "RallyAgg", "value_scale": "rallyagg_percentile_band"}},
            {"code": "Ratio", "header": "Rally Winners / Unforced", "source": {"kind": "group_average", "table": "group_008", "column": "Ratio"}},
            {"code": "FH_Wnr/Pt", "header": "Forehand Win %", "source": {"kind": "group_average", "table": "group_008", "column": "FH_Wnr/Pt"}},
            {"code": "BH_Wnr/Pt", "header": "Backhand Win %", "source": {"kind": "group_average", "table": "group_008", "column": "BH_Wnr/Pt"}},
        ],
    },
    {
        "sheet": "Attitude Analysis",
        "fields": [
            {"code": "SvStayMatch", "header": "Serve Stay/Match won", "source": {"kind": "group_average", "table": "group_011", "column": "SvStayMatch"}},
            {"code": "ReturnAgg", "header": "Return Aggressiveness", "source": {"kind": "group_average", "table": "group_016", "column": "ReturnAgg"}},
            {"code": "RallyAgg", "header": "Rally Aggressiveness", "source": {"kind": "group_average", "table": "group_016", "column": "RallyAgg", "value_scale": "rallyagg_percentile_band"}},
            {"code": "Consol%", "header": "Break Consol. %", "source": {"kind": "group_average", "table": "group_011", "column": "Consol%"}},
            {"code": "TB%", "header": "Tie Break won", "source": {"kind": "group_row", "table": "group_004", "column": "TB%", "row_column": "Year", "row_value": "Career", "source_like": CHALLENGER_CAREER_SOURCE}},
            {"code": "BreakBack%", "header": "Break Back %", "source": {"kind": "group_average", "table": "group_011", "column": "BreakBack%"}},
            {"code": "BP_Conv", "header": "Break Pts converted", "source": {"kind": "group_average", "table": "group_010", "column": "BP_Conv"}},
        ],
    },
    {
        "sheet": "Tactics Analysis",
        "fields": [
            {"code": "Drop:_Freq", "header": "Drop Freq.", "source": {"kind": "group_average", "table": "group_016", "column": "Drop:_Freq"}},
            {"code": "Net_Freq", "header": "Net Freq.", "source": {"kind": "group_average", "table": "group_016", "column": "Net_Freq"}},
            {"code": "SnV_Freq", "header": "Serve & Volley Freq.", "source": {"kind": "group_average", "table": "group_016", "column": "SnV_Freq"}},
            {"code": "crosscourt", "header": "Crosscourt %", "source": {"kind": "mcp_comparison", "stat": "crosscourt", "divide_by": 100}},
            {"code": "down_the_line", "header": "Down the Line %", "source": {"kind": "mcp_comparison", "stat": "down_the_line", "divide_by": 100}},
            {"code": "inside_in", "header": "Inside In %", "source": {"kind": "mcp_comparison", "stat": "inside_in", "divide_by": 100}},
            {"code": "inside_out", "header": "Inside Out %", "source": {"kind": "mcp_comparison", "stat": "inside_out", "divide_by": 100}},
        ],
    },
    {
        "sheet": "Efficiency Analysis",
        "fields": [
            {"code": "Ratio", "header": "Ratio W/UE", "source": {"kind": "group_average", "table": "group_008", "column": "Ratio"}},
            {"code": "RPW", "header": "Return Pts won", "source": {"kind": "group_row", "table": "group_004", "column": "RPW", "row_column": "Year", "row_value": "Career", "source_like": CHALLENGER_CAREER_SOURCE}},
            {"code": "Wnr/Pt", "header": "Winners", "source": {"kind": "group_average", "table": "group_008", "column": "Wnr/Pt"}},
            {"code": "SPW", "header": "Serve Pts won", "source": {"kind": "group_row", "table": "group_004", "column": "SPW", "row_column": "Year", "row_value": "Career", "source_like": CHALLENGER_CAREER_SOURCE}},
            {"code": "Hld%", "header": "Serve Games hold %", "source": {"kind": "group_row", "table": "group_004", "column": "Hld%", "row_column": "Year", "row_value": "Career", "source_like": CHALLENGER_CAREER_SOURCE}},
            {"code": "BP_Conv/BPG", "header": "Break vs Break Pts", "source": {"kind": "group_average", "table": "group_011", "column": "BP_Conv/BPG"}},
            {"code": "BP_Games", "header": "Games w/ Break Pts", "source": {"kind": "group_average", "table": "group_011", "column": "BP_Games"}},
        ],
    },
]

# =========================
# MCP TABLE MAPPING
# =========================
MCP_TABLE_BY_STAT = {
    "crosscourt": "mcp_m_stats_shotdirection_enriched",
    "down_the_line": "mcp_m_stats_shotdirection_enriched",
    "inside_in": "mcp_m_stats_shotdirection_enriched",
    "inside_out": "mcp_m_stats_shotdirection_enriched",
}

# =========================
# PARSING UTILS
# =========================
_PERCENT_RE = re.compile(r"(-?\d+(?:[.,]\d+)?)\s*%")
_FRACTION_RE = re.compile(r"(-?\d+(?:[.,]\d+)?)\s*/\s*(-?\d+(?:[.,]\d+)?)")
_NUMBER_RE = re.compile(r"-?\d+(?:[.,]\d+)?")


def _to_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace("\xa0", " ").strip()
    if text in ("", "NA", "-", "None", "NULL"):
        return None
    m = _PERCENT_RE.search(text)
    if m:
        return float(m.group(1).replace(",", "."))
    m = _FRACTION_RE.search(text)
    if m:
        num = float(m.group(1).replace(",", "."))
        den = float(m.group(2).replace(",", "."))
        return (num / den) * 100.0 if den != 0 else None
    m = _NUMBER_RE.search(text)
    if m:
        return float(m.group(0).replace(",", "."))
    return None


def _mean_or_none(values):
    clean = [v for v in values if v is not None]
    return sum(clean) / len(clean) if clean else None


def _clamp(value, lower, upper):
    return max(lower, min(upper, value))


def _percentile(values, pct):
    ordered = sorted(v for v in values if v is not None)
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * (pct / 100.0)
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    w = pos - lo
    return ordered[lo] * (1.0 - w) + ordered[hi] * w


# =========================
# SQL UTILS
# =========================
def sql_quote(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def table_exists(cur, name: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None


def _build_group_filters(source, player_name=None):
    clauses, params = [], []
    if player_name is not None:
        clauses.append(f'{sql_quote("__player__")} = ?')
        params.append(player_name)
    row_column = source.get("row_column")
    row_value = source.get("row_value")
    if row_column and row_value is not None:
        clauses.append(f"{sql_quote(row_column)} = ?")
        params.append(row_value)
    source_like = source.get("source_like")
    if source_like:
        clauses.append(f'{sql_quote("source_table")} LIKE ?')
        params.append(source_like)
    return clauses, params


# =========================
# GROUP QUERY FUNCTIONS
# =========================
def _group_row_numeric(conn, source, player_name):
    table, column = source["table"], source["column"]
    clauses, params = _build_group_filters(source, player_name)
    row = conn.execute(
        f"SELECT {sql_quote(column)} FROM {sql_quote(table)} WHERE {' AND '.join(clauses)} LIMIT 1",
        params,
    ).fetchone()
    return _to_float(row[0] if row else None)


def _group_row_average(conn, source):
    table, column = source["table"], source["column"]
    clauses, params = _build_group_filters(source)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    values_by_player: dict = defaultdict(list)
    for player_name, raw_value in conn.execute(
        f"SELECT {sql_quote('__player__')}, {sql_quote(column)} FROM {sql_quote(table)} {where_sql}", params
    ).fetchall():
        values_by_player[player_name].append(_to_float(raw_value))
    return _mean_or_none(_mean_or_none(v) for v in values_by_player.values())


def _group_average_numeric(conn, source, player_name):
    table, column = source["table"], source["column"]
    clauses, params = _build_group_filters(source, player_name)
    values = [
        _to_float(row[0])
        for row in conn.execute(
            f"SELECT {sql_quote(column)} FROM {sql_quote(table)} WHERE {' AND '.join(clauses)}", params
        ).fetchall()
    ]
    return _mean_or_none(values)


def _group_average_all_players(conn, source):
    table, column = source["table"], source["column"]
    clauses, params = _build_group_filters(source)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    values_by_player: dict = defaultdict(list)
    for player_name, raw_value in conn.execute(
        f"SELECT {sql_quote('__player__')}, {sql_quote(column)} FROM {sql_quote(table)} {where_sql}", params
    ).fetchall():
        values_by_player[player_name].append(_to_float(raw_value))
    return _mean_or_none(_mean_or_none(v) for v in values_by_player.values())


# =========================
# SCALING
# =========================
_ATP_AVERAGE_DISPLAY = "ATP average"
_VALUE_SCALE_CACHE: dict = {}


def _rallyagg_reference_stats(conn, source):
    table, column = source["table"], source["column"]
    clauses, params = _build_group_filters(source)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    values_by_player: dict = defaultdict(list)
    for player_name, raw_value in conn.execute(
        f"SELECT {sql_quote('__player__')}, {sql_quote(column)} FROM {sql_quote(table)} {where_sql}", params
    ).fetchall():
        v = _to_float(raw_value)
        if v is not None:
            values_by_player[player_name].append(v)
    distribution = [sum(v) / len(v) for v in values_by_player.values() if v]
    if not distribution:
        return None, None, None
    atp = sum(distribution) / len(distribution)
    return atp, _percentile(distribution, 10), _percentile(distribution, 90)


def _scale_rallyagg(x, atp, p10, p90):
    if x is None or atp is None or p10 is None or p90 is None:
        return None
    if x >= atp:
        denom = max(p90 - atp, 1e-9)
        scaled = 1.0 + 0.5 * ((x - atp) / denom)
    else:
        denom = max(atp - p10, 1e-9)
        scaled = 1.0 - 0.5 * ((atp - x) / denom)
    return _clamp(scaled, 0.5, 1.5)


def _source_cache_key(source):
    return (
        source.get("kind"), source.get("table"), source.get("column"),
        source.get("row_column"), source.get("row_value"),
        source.get("source_like"), source.get("value_scale"),
    )


def apply_value_scale(conn, raw_value, source):
    scale_kind = source.get("value_scale")
    if raw_value is None or scale_kind is None:
        return raw_value
    if scale_kind == "rallyagg_percentile_band":
        key = _source_cache_key(source)
        if key not in _VALUE_SCALE_CACHE:
            _VALUE_SCALE_CACHE[key] = _rallyagg_reference_stats(conn, source)
        atp, p10, p90 = _VALUE_SCALE_CACHE[key]
        return _scale_rallyagg(raw_value, atp, p10, p90)
    return raw_value


def source_numeric_value(conn, source, display_name, player_name):
    kind = source["kind"]
    if kind == "group_row":
        if display_name == _ATP_AVERAGE_DISPLAY:
            return _group_row_average(conn, source)
        return _group_row_numeric(conn, source, player_name)
    if kind == "group_average":
        if display_name == _ATP_AVERAGE_DISPLAY:
            return _group_average_all_players(conn, source)
        return _group_average_numeric(conn, source, player_name)
    raise ValueError(f"Unsupported source kind: {kind}")


# =========================
# MAIN LOGIC
# =========================
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
            numeric_value = source_numeric_for_top200(conn, source, player_key, columns_cache, mcp_name_map)
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