import pandas as pd
from etl.context import Context

def build_dim_waste(df: pd.DataFrame) -> pd.DataFrame:
    raw_cols = [
        'Do you produce waste water containing organic matter',
        'Waste water volume', 'UoM Waste water volume',
        'Oxygen demand', 'UoM Oxygen demand', 'Oxygen demand type',
        'Treatment Process'
    ]
    used_cols = [col for col in raw_cols if col in df.columns]
    Context.used_columns.update(used_cols)

    df = df[used_cols].drop_duplicates().reset_index(drop=True)
    rename_map = {
        'Do you produce waste water containing organic matter': 'organic_waste_flag',
        'Waste water volume': 'waste_volume',
        'UoM Waste water volume': 'uom_waste_volume',
        'Oxygen demand': 'oxygen_demand',
        'UoM Oxygen demand': 'uom_oxygen_demand',
        'Oxygen demand type': 'demand_type',
        'Treatment Process': 'treatment'
    }
    df.columns = [rename_map[c] for c in used_cols]
    df.insert(0, 'waste_id', df.index + 1)
    return df
