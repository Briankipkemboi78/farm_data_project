from etl.utils.matcher import match

def build_fact_economics(df):
    
    return df[[
        match(df, ['Entity ID']),
        match(df, ['1st main cash crop']),
        match(df, ['2nd main cash crop']),
        match(df, ['Other crop 1']),
        match(df, ['Other crop 2']),
        match(df, ['Livestock']),
        match(df, ['sell your coffee'])
    ]].copy().rename(columns=lambda x: x.lower().replace(" ", "_")).drop_duplicates().reset_index(drop=True)
