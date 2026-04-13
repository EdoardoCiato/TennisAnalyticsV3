import sqlite3

DB_PATH = "tennis_abstract_new_version_merged_testing.db"

def is_percent_column(cursor, table, col):
    """
    Verifica se una colonna contiene valori percentuali tipo '72.3%'
    """
    cursor.execute(f'''
        SELECT "{col}"
        FROM "{table}"
        WHERE "{col}" IS NOT NULL
          AND TRIM("{col}") NOT IN ('', 'NA')
        LIMIT 1
    ''')
    row = cursor.fetchone()
    if not row:
        return False
    value = str(row[0]).strip()
    return value.endswith('%')

def avg_expression(col):
    """
    Costruisce espressione SQL per fare AVG ignorando NA e rimuovendo '%'
    """
    return f'''
        AVG(
            CASE
                WHEN TRIM("{col}") IN ('', 'NA') THEN NULL
                ELSE CAST(REPLACE("{col}", '%', '') AS REAL)
            END
        )
    '''

with sqlite3.connect(DB_PATH) as conn:
    cur = conn.cursor()

    # Prendi colonne da general
    cur.execute('PRAGMA table_info("general")')
    cols = [r[1] for r in cur.fetchall()]

    # Ricrea tabella
    cur.execute('DROP TABLE IF EXISTS "global_averages"')
    cur.execute('CREATE TABLE "global_averages" ("player_name" TEXT)')

    # Tutte colonne TEXT (formato report)
    for c in cols:
        if c != "player_name":
            cur.execute(f'ALTER TABLE "global_averages" ADD COLUMN "{c}" TEXT')

    values = {"player_name": "ATP average"}

    for c in cols:
        if c == "player_name":
            continue

        percent = is_percent_column(cur, "general", c)

        cur.execute(f'SELECT {avg_expression(c)} FROM "general"')
        value = cur.fetchone()[0]

        if value is None:
            values[c] = None
        else:
            if percent:
                # Percentuali → 1 decimale + simbolo %
                values[c] = f"{round(value, 1)}%"
            else:
                # Altri numeri → 2 decimali
                values[c] = f"{round(value, 2)}"

    insert_cols = list(values.keys())
    placeholders = ",".join(["?"] * len(insert_cols))
    col_sql = ",".join([f'"{c}"' for c in insert_cols])

    cur.execute(
        f'INSERT INTO "global_averages" ({col_sql}) VALUES ({placeholders})',
        [values[c] for c in insert_cols]
    )

    conn.commit()

print("✅ global_averages report table created correctly.")