
# tennis_scrape_to_sqlite_debug.py

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

# =========================
# CONFIG
# =========================
DB_PATH = "tennis_abstract_raw.db"
BASE_CLASSIC = "https://www.tennisabstract.com/cgi-bin/player.cgi?p={player}"
PAGE_LOAD_TIMEOUT = 25
SLEEP_AFTER_LOAD = 2.0   # increased
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

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

    return driver


def load_page_html(driver: webdriver.Chrome, url: str) -> Tuple[str, str]:
    print(f"\n🌐 Loading: {url}")
    driver.get(url)

    try:
        WebDriverWait(driver, 20).until(
            lambda d: len(d.find_elements(By.TAG_NAME, "table")) >= 1
        )
    except Exception:
        print("⚠️ Timeout waiting for tables")

    time.sleep(SLEEP_AFTER_LOAD)

    # DEBUG INFO
    print("Title:", driver.title)
    print("URL:", driver.current_url)
    print("Browser:", driver.capabilities.get("browserVersion"))
    print("Tables (selenium):", len(driver.find_elements(By.TAG_NAME, "table")))
    print("User agent:", driver.execute_script("return navigator.userAgent"))

    html = driver.page_source
    return html, driver.title

# =========================
# TABLE EXTRACTION
# =========================
def extract_tables_from_html(html: str) -> List[Tuple[str, pd.DataFrame]]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")

    print(f"🔍 Found {len(tables)} <table> tags in HTML")

    out: List[Tuple[str, pd.DataFrame]] = []
    count = 0

    for idx, table in enumerate(tables, start=1):
        try:
            df = pd.read_html(StringIO(str(table)))[0]

            if df.empty:
                print(f"Table {idx}: empty")
                continue

            df = safe_columns(df)

            label = f"Table_{count + 1}"
            count += 1

            out.append((label, df))

        except Exception as e:
            print(f"❌ Table {idx} parse error: {e}")

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
        for player in players:
            player = player.strip()
            if not player:
                continue

            url = BASE_CLASSIC.format(player=player)
            print(f"\n====================")
            print(f"PLAYER: {player}")

            try:
                html, title = load_page_html(driver, url)
                tables = extract_tables_from_html(html)

                if not tables:
                    print("⚠️ No tables extracted")

                    # SAVE DEBUG HTML
                    with open(f"debug_{player}.html", "w", encoding="utf-8") as f:
                        f.write(html)

                    print(f"💾 Saved debug HTML: debug_{player}.html")

                    missing_players.append(player)
                    continue

                extracted_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

                for i, (label, df) in enumerate(tables, start=1):
                    table_name = normalize_identifier(f"{player}_{label}")

                    df["__player__"] = player

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
                            label,
                            table_name,
                            int(df.shape[0]),
                            int(df.shape[1]),
                            extracted_at,
                        )
                    )

                    conn.commit()

                    print(f"✅ Saved: {table_name} ({df.shape[0]}x{df.shape[1]})")

            except Exception as e:
                print(f"❌ Error on {player}: {e}")

            time.sleep(POLITE_SLEEP_BETWEEN_PLAYERS)

    finally:
        driver.quit()
        conn.close()

    print("\n✅ DONE")
    print("Missing players:", missing_players)

# =========================
# RUN
# =========================
if __name__ == "__main__":

    players = []
    with open("top200.txt") as file:
        for line in file:
            players.append(line.strip())

    scrape_players_to_sqlite(players, headless=True)

