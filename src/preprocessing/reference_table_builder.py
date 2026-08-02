import pandas as pd
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.config import DB_PATH

# TODO: re-do completely. Find new/smarter way. 

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute(''' DROP TABLE "reference_table"''')

cursor.execute(''' \
   CREATE TABLE IF NOT EXISTS "reference_table" ( \
  "Variable_name" TEXT PRIMARY KEY, \
   "Parity" INTEGER, \
   "Description" TEXT, \
   "Topic" TEXT, \
   "Efficiency" INTEGER, \
   "Table_position" INTEGER, \
   "In_chart" INTEGER, \
   "Reference_table" TEXT, \
   "filter_date" TEXT,
   "JS" INTEGER,
   "Is_ratio" INTEGER          
   )''')


ref_table = pd.read_excel('data/raw/Reference_Table.xlsx',  header = 1, usecols = 'A:K', dtype = 'object')
for i, row in ref_table.iterrows():
    placeholders = ', '.join(map(str, row.to_list()))
    placeholders = ", ".join(f"'{word.strip()}'" for word in placeholders.split(','))
    cursor.execute(f'''
  INSERT INTO "reference_table" VALUES ({placeholders}) 
                     ''')

conn.commit()