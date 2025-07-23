from etl.utils.matcher import match
import pandas as pd

def build_dim_education(df: pd.DataFrame) -> pd.DataFrame:
    level_col = match(df, ['Education Level', 'Highest education attained'])
    out = df[[level_col]].dropna().drop_duplicates().reset_index(drop=True)
    out.columns = ['education_level']
    out['education_id'] = range(1, len(out) + 1)
    return out[['education_id', 'education_level']]
