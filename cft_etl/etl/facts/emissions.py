import pandas as pd
from etl.context import Context

def build_fact_emissions(df: pd.DataFrame) -> pd.DataFrame:
    emission_cols = [col for col in df.columns if any(gas in col for gas in ['CO2', 'CH4', 'N2O', 'CO2e'])]
    used_cols = ['result_id'] + emission_cols
    Context.used_columns.update(used_cols)
    df = df[used_cols].copy()
    clean_names = ['result_id'] + [
        col.strip().lower().replace(' ', '_').replace('*', '').replace('(', '').replace(')', '')
        for col in emission_cols
    ]
    df.columns = clean_names
    df.insert(0, 'emissions_id', df.index + 1)
    return df.drop_duplicates().reset_index(drop=True)
