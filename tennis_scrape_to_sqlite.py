# tennis_scrape_to_sqlite.py
# Step 1: Scrapes all tables from Tennis Abstract classic player pages.
# Saves each table to SQLite with metadata in tables_index.

from __future__ import annotations
import os
import re
import time
import sqlite3
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
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "tennis_abstract_new_version.db")
BASE_CLASSIC = "https://www.tennisabstract.com/cgi-bin/player.cgi?p={player}"
PAGE_LOAD_TIMEOUT = 25
SLEEP_AFTER_LOAD = 3.0
POLITE_SLEEP_BETWEEN_PLAYERS = 3.0

# Retry con backoff
MAX_RETRIES = 3
BACKOFF_BASE = 5.0

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


def deduplicate_headers(headers: list) -> list:
    """
    ✅ FIX: in caso di colonne duplicate, tiene l'ULTIMA occorrenza
    (la nuova colonna sovrascrive la precedente).
    Le occorrenze precedenti vengono marcate con __drop_{i}__ e poi eliminate.
    """
    # Prima passata: trova l'indice dell'ultima occorrenza per ogni nome
    last_occurrence = {}
    for i, h in enumerate(headers):
        h = h.strip() if h else "col"
        last_occurrence[h] = i  # sovrascrive sempre → tiene l'ultimo

    # Seconda passata: assegna i nomi finali
    out = []
    for i, h in enumerate(headers):
        h = h.strip() if h else "col"
        if last_occurrence[h] == i:
            # ✅ ultima occorrenza → tieni il nome originale
            out.append(h)
        else:
            # occorrenza precedente → placeholder da eliminare dopo
            out.append(f"__drop_{i}__")
    return out


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

    # Anti-bot
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

    # Rimuove il flag navigator.webdriver
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"}
    )

    return driver


def load_page_html(driver: webdriver.Chrome, url: str) -> Tuple[str, str]:
    driver.get(url)

    try:
        WebDriverWait(driver, 20).until(
            lambda d: len(d.find_elements(By.TAG_NAME, "table")) >= 3
        )
    except Exception:
        print("   ⚠️ Timeout attesa tabelle — procedo comunque")

    time.sleep(SLEEP_AFTER_LOAD)

    # Scroll per forzare il caricamento lazy
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(1.0)
    driver.execute_script("window.scrollTo(0, 0);")

    if "access denied" in driver.title.lower() or "cloudflare" in driver.page_source.lower():
        print("   🚨 ATTENZIONE: il sito potrebbe aver bloccato la richiesta!")

    html = driver.page_source
    title = driver.title
    return html, title


# =========================
# TABLE EXTRACTION
# =========================
def extract_tables_from_html(html: str) -> List[Tuple[str, pd.DataFrame]]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    print(f"   🔍 Trovate {len(tables)} tabelle nell'HTML")

    out: List[Tuple[str, pd.DataFrame]] = []
    count = 0

    for idx, table in enumerate(tables, start=1):
        try:
            headers = []
            rows = []

            thead = table.find("thead")
            if thead:
                headers = [th.get_text(strip=True) for th in thead.find_all(["th", "td"])]

            tbody = table.find("tbody") or table
            for tr in tbody.find_all("tr"):
                cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                if cells:
                    rows.append(cells)

            if not rows:
                continue

            if not headers and rows:
                headers = rows[0]
                rows = rows[1:]

            if not rows:
                continue

            # Normalizza lunghezza righe
            max_cols = max(len(headers), max(len(r) for r in rows))
            headers = headers + [f"col_{i}" for i in range(len(headers), max_cols)]
            rows = [r + [""] * (max_cols - len(r)) for r in rows]

            # ✅ FIX: deduplica headers — tiene l'ultima occorrenza
            headers = deduplicate_headers(headers)

            df = pd.DataFrame(rows, columns=headers)

            # ✅ FIX: elimina le colonne placeholder dei duplicati precedenti
            drop_cols = [c for c in df.columns if c.startswith("__drop_")]
            if drop_cols:
                df.drop(columns=drop_cols, inplace=True)

            if df.empty or df.shape[1] < 2:
                continue

            df = safe_columns(df)

            # Label dalla <h1> precedente
            label_tag = None
            for prev in table.find_all_previous():
                if prev.name == "h1":
                    label_tag = prev
                    break

            label = label_tag.get_text(strip=True) if label_tag else f"Unknown_Table_{count + 1}"
            count += 1
            out.append((label, df))

        except Exception as e:
            print(f"   ❌ Tabella {idx} errore: {e}")
            continue

    return out


# =========================
# RETRY con backoff esponenziale
# =========================
def scrape_player_with_retry(
    driver: webdriver.Chrome,
    player: str,
    conn: sqlite3.Connection,
) -> bool:
    """
    Tenta di scrapare un giocatore fino a MAX_RETRIES volte.
    Ritorna True se ha avuto successo, False altrimenti.
    Backoff: retry 1 → 5s, retry 2 → 10s, retry 3 → 20s
    """
    url = BASE_CLASSIC.format(player=player)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"   🔄 Tentativo {attempt}/{MAX_RETRIES}")

            html, title = load_page_html(driver, url)
            tables = extract_tables_from_html(html)

            if not tables:
                raise ValueError("Nessuna tabella estratta")

            extracted_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

            for i, (label, df) in enumerate(tables, start=1):
                table_label = label or f"Table_{i}"
                table_name = normalize_identifier(f"{player}_{table_label}")
                df["__player__"] = player

                df.to_sql(table_name, conn, if_exists="replace", index=False)

                conn.execute(
                    """
                    INSERT INTO tables_index
                    (player, source_url, page_title, table_no, table_label, sqlite_table, rows, cols, extracted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        player, url, title, i, table_label, table_name,
                        int(df.shape[0]), int(df.shape[1]), extracted_at,
                    )
                )
                conn.commit()
                print(f"   ✅ Salvata: {table_name} ({df.shape[0]}x{df.shape[1]})")

            return True  # ✅ Successo

        except Exception as e:
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            print(f"   ❌ Tentativo {attempt} fallito: {e}")

            if attempt < MAX_RETRIES:
                print(f"   ⏳ Attendo {wait:.0f}s prima del prossimo tentativo...")
                time.sleep(wait)
            else:
                print(f"   🚫 Tutti i {MAX_RETRIES} tentativi falliti per: {player}")

    return False


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

            print(f"\n====================")
            print(f"PLAYER: {player}")
            print(f"URL: {BASE_CLASSIC.format(player=player)}")

            success = scrape_player_with_retry(driver, player, conn)

            if not success:
                missing_players.append(player)
                print(f"   ⚠️ Aggiunto a missing_players: {missing_players}")

                # Salva debug HTML per i falliti
                try:
                    with open(os.path.join(BASE_DIR, f"debug_{player}.html"), "w", encoding="utf-8") as f:
                        f.write(driver.page_source)
                    print(f"   💾 Salvato debug HTML: debug_{player}.html")
                except Exception:
                    pass

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

    top200_path = os.environ.get("PLAYER_LIST_FILE") or os.path.join(BASE_DIR, "top200.txt")

    players = []
    with open(top200_path, encoding="utf-8") as file:
        for line in file:
            players.append(line.strip())

    scrape_players_to_sqlite(players, db_path=DB_PATH, headless=True)

    # Riprova i giocatori falliti
    while missing_players:
        print(f"\n🔁 Riprovo {len(missing_players)} giocatori mancanti...")
        current_missing = missing_players
        missing_players = []
        scrape_players_to_sqlite(current_missing, db_path=DB_PATH, headless=True)
