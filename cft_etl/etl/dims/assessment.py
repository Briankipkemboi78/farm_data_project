import pandas as pd
from etl.context import Context

def build_dim_assessment(df: pd.DataFrame) -> pd.DataFrame:
    used_cols = ['Assesment UID', 'Assessment name', 'FA Name']
    Context.used_columns.update(used_cols)
    assess_df = df[used_cols].copy()
    assess_df.columns = ['assessment_uid', 'assessment_name', 'fa_name']
    return assess_df.drop_duplicates().reset_index(drop=True)