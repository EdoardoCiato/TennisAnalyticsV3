
import pandas as pd
import os
def export_tables_by_category(tables, rows, output_file="tables.xlsx", ):
    sheet_positions = {}

    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        for table_id, table in tables.items():
            category = table_id[0]   # e.g. "Serve"
            table_type = table_id[1] # e.g. "chart", "full", "scaled"

            # first table in this sheet starts at col 0
            if category not in sheet_positions:
                sheet_positions[category] = 0

            startcol = sheet_positions[category]
            if table_type != 'scaled':
                table = format(table, rows)
            table.to_excel(
                writer,
                sheet_name=category,
                startrow=1,
                startcol=startcol,
                index=True
            )

            # optional title above the table
            worksheet = writer.sheets[category]
            worksheet
            worksheet.write(0, startcol + 1, table_type)

            # move to the right for next table in same sheet
            sheet_positions[category] += len(table.columns) + 2

def format(df, rows):
    df_format = df.copy().T
    for col_name in df_format.columns:
        if is_ratio(col_name, rows):
            df_format[col_name] = df_format[col_name].apply(lambda value: turn_ratio(value))


    return df_format.T

def turn_ratio(value):
    if value >= -1 and value <= 1:
        return str(round(value *100,1))+'%'
    else:
        return str(value)+'%'

def is_ratio(col, rows):
    for ind_dict in rows:
        if ind_dict['column_name'] == col and ind_dict['is_ratio'] == 1:
            return True
        elif col == 'Delta':
            return True
        
    return False