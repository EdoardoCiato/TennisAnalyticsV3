
import pandas as pd
rows = [
    (45, 120, 35),
    (30, 95, 25),
    (50, 140, 40),
    (35, 110, 30),
    (40, 125, 45)
]

cols = ["shallow", "deep", "very_deep"]

df = pd.DataFrame(rows, columns=cols)
#columns-wise sum --> sum of an indicator over all the matches recorded
totals = df.sum(numeric_only=True)
# row-wise sum --> total number of forehands or backhands
side_total = totals.sum()
if side_total == 0:
    print( {col: None for col in cols})
print(dict(zip(cols, round(totals/side_total*100,2))))