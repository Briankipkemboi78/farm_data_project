import pandas as pd

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Replace "-" and empty strings with NaN for uniformity
    df.replace("-", pd.NA, inplace=True)
    df.replace("", pd.NA, inplace=True)
    df = df.dropna(axis=1, how='all')

    return df
