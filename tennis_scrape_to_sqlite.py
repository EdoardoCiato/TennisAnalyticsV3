# tennis_scrape_to_sqlite.py
# Step 1: Scrapes all tables from Tennis Abstract classic player pages.
# Saves each table to SQLite with metadata in tables_index.

from __future__ import annotations
import re
import time
import sqlite3
from io import StringIO
from datetime import datetime
from typing import Iterable, List, Tuple

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
# CONFIG
# =========================
DB_PATH = "data/tennis_abstract_new_version_testing.db"
BASE_CLASSIC = "https://www.tennisabstract.com/cgi-bin/player.cgi?p={player}"
PAGE_LOAD_TIMEOUT = 25
SLEEP_AFTER_LOAD = 0.8
POLITE_SLEEP_BETWEEN_PLAYERS = 1.2
missing_players = []
# =========================
# UTILS
# =========================
def normalize_identifier(s: str) -> str:
    s = re.sub(r"\s+", "_", str(s).strip())
    s = re.sub(r"[^A-Za-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:60] if s else "x"

def safe_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [str(c).strip().replace("\n", " ").replace("\r", " ") for c in df.columns]
    cols = [re.sub(r"\s+", " ", c) for c in cols]
    cols2 = [re.sub(r"\s+", "_", c) if c else "col" for c in cols]
    seen = {}
    out = []
    for c in cols2:
        if c not in seen:
            seen[c] = 1
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
    df.columns = out
    return df

def ensure_index_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tables_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player TEXT NOT NULL,
            source_url TEXT NOT NULL,
            page_title TEXT,
            table_no INTEGER NOT NULL,
            table_label TEXT,
            sqlite_table TEXT NOT NULL,
            rows INTEGER NOT NULL,
            cols INTEGER NOT NULL,
            extracted_at TEXT NOT NULL
        )
        """
    )
    conn.commit()

# =========================
# SELENIUM
# =========================
def make_driver(headless: bool = True) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1400,900")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver

def load_page_html(driver: webdriver.Chrome, url: str) -> Tuple[str, str]:
    driver.get(url)
    try:
        WebDriverWait(driver, 18).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
    except Exception:
        pass
    time.sleep(SLEEP_AFTER_LOAD)
    html = driver.page_source
    title = driver.title
    return html, title

# =========================
# TABLE EXTRACTION
# =========================
def extract_tables_from_html(html: str) -> List[Tuple[str, pd.DataFrame]]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    out: List[Tuple[str, pd.DataFrame]] = []
    count = 0

    for table in tables:
        try:
            df = pd.read_html(StringIO(str(table)))[0]
            #df.dropna(axis=0, how="all", inplace=True)
            #df.dropna(axis=1, how="all", inplace=True)
            if df.empty:
                continue
            df = safe_columns(df)

            # Try to find the preceding <h1> as label
            label_tag = None
            for prev in table.find_all_previous():
                if prev.name == "h1":
                    label_tag = prev
                    break

            label = label_tag.get_text(strip=True) if label_tag else f"Unknown Table {count + 1}"
            count += 1
            out.append((label, df))
        except Exception:
            continue

    return out

# =========================
# MAIN SCRAPER
# =========================
def scrape_players_to_sqlite(
    players: Iterable[str],
    db_path: str = DB_PATH,
    headless: bool = True
) -> None:
    conn = sqlite3.connect(db_path)
    ensure_index_table(conn)
    driver = make_driver(headless=headless)
    
    global missing_players
    try:
        print(players)
        for player in players:
            player = player.strip()
            if not player:
                continue

            url = BASE_CLASSIC.format(player=player)
            print(f"\n==> {player} | {url}")

            try:
                html, title = load_page_html(driver, url)
                tables = extract_tables_from_html(html)

                if not tables:
                    print("   ⚠️ Nessuna tabella trovata.")
                    missing_players.append(player)
                    print("Missing players:", missing_players)
                    continue

                extracted_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

                for i, (label, df) in enumerate(tables, start=1):
                    table_label = label or f"Table {i}"
                    table_name = normalize_identifier(f"{player}_{table_label}")
                    df["__player__"] = player  # helpful for later merging

                    df.to_sql(table_name, conn, if_exists="replace", index=False)

                    conn.execute(
                        """
                        INSERT INTO tables_index
                        (player, source_url, page_title, table_no, table_label, sqlite_table, rows, cols, extracted_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            player,
                            url,
                            title,
                            i,
                            table_label,
                            table_name,
                            int(df.shape[0]),
                            int(df.shape[1]),
                            extracted_at,
                        )
                    )
                    conn.commit()
                    print(f"   ✅ Salvata: {table_name} ({df.shape[0]}x{df.shape[1]})")

            except Exception as e:
                print(f"   ❌ Errore su {player}: {e}")

            time.sleep(POLITE_SLEEP_BETWEEN_PLAYERS)

    finally:
        driver.quit()
        conn.close()

    print(f"\n✅ Completato. DB SQLite: {db_path}")
    print("Missing players:", missing_players)

# =========================
# RUN HERE
# =========================
if __name__ == "__main__":
    
    players = []
    with open("data/top20.txt") as file:
        for line in file:
             players.append(line)
    PLAYERS  = players
    scrape_players_to_sqlite(PLAYERS, db_path=DB_PATH, headless=True)

    while missing_players:
        current_missing_players = missing_players
        missing_players = []
        scrape_players_to_sqlite(current_missing_players, db_path=DB_PATH, headless=True)