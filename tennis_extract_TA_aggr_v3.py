import sqlite3
import pandas as pd
from collections import defaultdict
from hashlib import md5
import json

DB_IN = "tennis_abstract_new_version.db"
DB_OUT = "tennis_abstract_new_version_merged.db"

EXCLUDE_TABLES = {"tables_index", "sqlite_sequence"}

# === STEP 1: Load all table names ===
def get_user_tables(conn):
    query = """
    SELECT name FROM sqlite_master
    WHERE type='table' AND name NOT LIKE 'sqlite_%'
    """
    all_tables = pd.read_sql_query(query, conn)["name"].tolist()
    return [t for t in all_tables if t not in EXCLUDE_TABLES]

# 1. Query the players from the index

# === STEP 2: Build schema signatures ===
def get_table_schema_signature(conn, table_name):
    cols = pd.read_sql_query(f'PRAGMA table_info("{table_name}")', conn)["name"].tolist()
    return tuple(cols)

# === STEP 3: Structure-based merging ===
def merge_by_structure():
    conn = sqlite3.connect(DB_IN)
    out_conn = sqlite3.connect(DB_OUT)

    # ✅ Step 1 — Create player_id mapping from tables_index
    all_names = pd.read_sql_query("SELECT DISTINCT player FROM tables_index", conn)
    all_names["player_id"] = range(1, len(all_names) + 1)
    all_names.rename(columns={"player": "player_name"}, inplace=True)
    all_names.to_sql("players_dim", conn, if_exists="replace", index=False)

    # ✅ Load player_id map
    players_dim = pd.read_sql_query("SELECT * FROM players_dim", conn)
    player_id_map = dict(zip(players_dim["player_name"], players_dim["player_id"]))

    # ✅ Step 2 — Group tables by structure
    tables = get_user_tables(conn)
    schema_groups = defaultdict(list)
    table_schemas = {}

    for tname in tables:
        schema = get_table_schema_signature(conn, tname)
        schema_hash = md5(json.dumps(schema).encode()).hexdigest()
        schema_groups[schema_hash].append(tname)
        table_schemas[tname] = schema

    merge_index = []

    # ✅ Step 3 — Merge each group
    for i, (schema_hash, table_list) in enumerate(schema_groups.items(), start=1):
        group_name = f"group_{i:03d}"
        merged_frames = []

        for tname in table_list:
            df = pd.read_sql_query(f'SELECT * FROM "{tname}"', conn)
            df["source_table"] = tname

            # Get label (optional)
            try:
                label_df = pd.read_sql_query(
                    "SELECT table_label FROM tables_index WHERE sqlite_table = ? LIMIT 1",
                    conn, params=(tname,))
                label = label_df["table_label"].iloc[0] if not label_df.empty else ""
            except Exception:
                label = ""

            df["table_label"] = label
            merged_frames.append(df)

        final_df = pd.concat(merged_frames, ignore_index=True)

        # ✅ Add player_id from player_name
        if "player_name" in final_df.columns:
            final_df["player_id"] = final_df["player_name"].map(player_id_map)

            # Move player_id to first column (optional)
            cols = final_df.columns.tolist()
            if "player_id" in cols:
                cols.insert(0, cols.pop(cols.index("player_id")))
                final_df = final_df[cols]

        # ✅ Save merged table
        final_df.to_sql(group_name, out_conn, if_exists="replace", index=False)

        # ✅ Record what we merged
        merge_index.append({
            "group_name": group_name,
            "num_tables": len(table_list),
            "num_rows": final_df.shape[0],
            "num_cols": final_df.shape[1],
            "columns": json.dumps(table_schemas[table_list[0]]),
            "source_tables": json.dumps(table_list)
        })

    # ✅ Save the group index
    pd.DataFrame(merge_index).to_sql("merge_groups_index", out_conn, if_exists="replace", index=False)

    conn.close()
    out_conn.close()
    print(f"✅ Phase 2 complete — merged DB saved as: {DB_OUT}")

if __name__ == "__main__":
    merge_by_structure()
