def build_dim_education(df):
    from etl.utils.matcher import match
    level_col = match(df, ['Education Level'])
    out = df[[level_col]].dropna().drop_duplicates().reset_index(drop=True)
    out.columns = ['education_level']
    out['education_id'] = range(1, len(out)+1)
    return out[['education_id', 'education_level']]
