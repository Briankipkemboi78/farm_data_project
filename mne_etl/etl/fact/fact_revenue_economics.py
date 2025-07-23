from etl.utils.matcher import match

def build_fact_revenue_economics(df):
    
    return df[[
        match(df, ['Entity ID']),
        match(df, ['Yield GC per ha']),
        match(df, ['production kg']),
        match(df, ['price']),
        match(df, ['total fertilizer applied kg per ha']),
        match(df, ['organic fertilizer applied kg per ha'])
    ]].copy().rename(columns=lambda x: x.lower().replace(" ", "_")).drop_duplicates().reset_index(drop=True)
