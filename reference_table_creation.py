import pandas as pd
import sqlite3

conn = sqlite3.connect('tennis_abstract_new_version_merged_testing.db')
cursor = conn.cursor()

#cursor.execute(''' DROP TABLE "reference_table"''')

cursor.execute(''' \
   CREATE TABLE IF NOT EXISTS "reference_table" ( \
  variable_name TEXT PRIMARY KEY, \
   parity INTEGER, \
   Description TEXT, \
   Topic TEXT, \
   Efficiency INTEGER, \
   Table_position INTEGER, \
   in_chart INTEGER, \
   reference_table TEXT, \
   filter_date TEXT,
   JS TEXT
   )''')


ref_table = pd.read_excel('Reference_Table.xlsx',  header = 1, usecols = 'A:J', dtype = 'object')
for i, row in ref_table.iterrows():
    placeholders = ', '.join(map(str, row.to_list()))
    placeholders = ", ".join(f"'{word.strip()}'" for word in placeholders.split(','))
    cursor.execute(f'''
  INSERT INTO "reference_table" VALUES ({placeholders}) 
                     ''')

conn.commit()