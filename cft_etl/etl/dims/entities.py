import pandas as pd
from etl.context import Context

def build_dim_entities(df: pd.DataFrame) -> pd.DataFrame:
    cols = ['EntitySystemID', 'EntityName', 'Interviewee']
    Context.used_columns.update(cols)
    df = df[cols].drop_duplicates().reset_index(drop=True)
    df.columns = ['entity_system_id', 'entity_name', 'interviewee']
    df.insert(0, 'entity_id', df.index + 1)
    return df



