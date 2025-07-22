import pandas as pd
from etl.context import Context

def build_fact_land_use(df: pd.DataFrame) -> pd.DataFrame:
    prior_cols = [col for col in df.columns if col.startswith('Prior')]
    used_cols = ['result_id'] + prior_cols
    Context.used_columns.update(used_cols)

    df = df[used_cols].drop_duplicates().reset_index(drop=True)
    df.columns = ['result_id'] + [
        col.strip().lower().replace(' ', '_').replace('-', '_')
        for col in prior_cols
    ]
    df.insert(0, 'land_use_id', df.index + 1)
    return df
