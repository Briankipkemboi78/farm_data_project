from etl.utils.matcher import match

def build_fact_biodiversity_assessment(df):
    
    return df[[
        match(df, ['Entity ID']),
        match(df, ['bee hives']),
        match(df, ['biodiversity habitat']),
        match(df, ['agroforestry']),
        match(df, ['shade trees']),
        match(df, ['native']),
        match(df, ['n-fixing']),
        match(df, ['tree species']),
        match(df, ['non-coffee trees'])
    ]].copy().rename(columns=lambda x: x.lower().replace(" ", "_")).drop_duplicates().reset_index(drop=True)
