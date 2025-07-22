import pandas as pd
from etl.context import Context

def build_dim_soil(df: pd.DataFrame) -> pd.DataFrame:
    cols = ['Soil texture', 'Soil organic matter (%)', 'Soil Classification',
            'Soil organic carbon (%)', 'Soil pH', 'Soil moisture average', 'Soil drainage']
    Context.used_columns.update(cols)
    df = df[cols].drop_duplicates().reset_index(drop=True)
    df.columns = ['texture', 'organic_matter', 'classification', 'organic_carbon',
                  'ph', 'moisture', 'drainage']
    df.insert(0, 'soil_id', df.index + 1)
    return df
