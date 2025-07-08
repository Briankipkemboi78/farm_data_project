import pandas as pd

def clean_entity_id(df: pd.DataFrame, column_name: str = 'Entity ID') -> pd.DataFrame:
    df[column_name] = df[column_name].astype(str).str.lstrip("'")
    return df
