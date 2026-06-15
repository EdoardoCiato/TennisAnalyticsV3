
def serve_direction_JS(cursor, indicator, reference_group, player ):
    if indicator in ["deuce_wide", "deuce_middle", "deuce_t"]:
        cols = ["deuce_wide", "deuce_middle", "deuce_t"]
    else:
        cols = ["ad_wide", "ad_middle", "ad_t"]
    
    query = f'''
                SELECT {", ".join(f'"{c}"' for c in cols)}
                FROM "{reference_group}"
                WHERE "player" = ? and "row" = "Total"
                    ''' 
    
    cursor.execute(query, (player,))
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    #columns-wise sum --> sum of an indicator over all the matches recorded
    totals = df.sum(numeric_only=True)
    # row-wise sum --> total number of serves from ad or deuce side
    side_total = totals.sum()
    if side_total == 0:
        return( {col: None for col in cols})
    return ( {col: round(totals[col] / side_total * 100, 1) for col in cols})

def shot_direction_JS(cursor, indicator, reference_group, player):
    if "FH" in indicator:
        cols = ["crosscourt", "down_middle", "down_the_line", "inside_out", "inside_in"]
        row = 'F'
        suffix = '_FH'
    else:
        cols = ["crosscourt", "down_middle", "down_the_line"]
        row = 'B'
        suffix = '_BH'
    
    query = f'''
                SELECT {", ".join(f'"{c}"' for c in cols)} FROM "{reference_group}"
                WHERE "player" = ? and "row" = ?
                    ''' 
    cursor.execute(query, (player, row))
    
    rows = cursor.fetchall()
    cols = list(map(lambda x: x + suffix, cols))
    return rows, cols

def data_aggregation_JS(indicator, player, reference_group, cursor):
   
    if reference_group == 'mcp_m_stats_shotdirection':
        rows, cols = shot_direction_JS(cursor, indicator, reference_group, player)
    elif reference_group == 'mcp_m_stats_servedirection':
        rows = serve_direction_JS(cursor,indicator,reference_group,player)

    df = pd.DataFrame(rows, columns=cols)
    #columns-wise sum --> sum of an indicator over all the matches recorded
    totals = df.sum(numeric_only=True)
    # row-wise sum --> total number of forehands or backhands
    side_total = totals.sum()
    if side_total == 0:
        return( {col: None for col in cols})
    return(dict(zip(cols, round(totals/side_total*100,2))))

    if reference_group == 'mcp_m_stats_servedirection':
        x = serve_direction_JS(cursor, indicator, reference_group, player )
    if reference_group == "mcp_m_stats_returndepth":
        # Returndepth values are absolute values, not a percentage
        query = f'''
            SELECT "{indicator}", "returnable" FROM "{reference_group}"
            WHERE "player" = ? and "row" = "Total"
                '''
    
    cursor.execute(query, (player,))
    rows = cursor.fetchall()
    if rows == []: 
        return None
    num = 0
    den = 0
    # No career values, so we calculate the average over the career
    for pair in rows:
        num += pair[0]
        try :
            den += pair[1]
        except IndexError:
            den = 0
    if len(rows[0]) > 1:
        value = round((num / den) * 100, 1)
    else:
        value = round(num/(len(rows)), 1)

    return (value)