import pandas as pd
from etl.context import Context

def build_dim_climate(df: pd.DataFrame) -> pd.DataFrame:
    cols = ['Climate', 'Annual average temperature', 'Intensity']
    Context.used_columns.update(cols)
    df = df[cols].drop_duplicates().reset_index(drop=True)
    df.columns = ['climate_zone', 'avg_temperature', 'intensity']
    df.insert(0, 'climate_id', df.index + 1)
    return df
