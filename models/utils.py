import pandas as pd

def clean_entity_id(df: pd.DataFrame, column_name: str = "Entity ID") -> pd.DataFrame:
    """
    Ensures entity ID is treated as string and strips leading apostrophes.
    """
    df[column_name] = df[column_name].astype(str).str.lstrip("'")
    return df
