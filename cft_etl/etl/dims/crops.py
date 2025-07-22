import pandas as pd
from etl.context import Context

def build_dim_crops(df: pd.DataFrame) -> pd.DataFrame:
    cols = ['Crop Name', 'Harvest year', 'Crop area', 'UoM Crop Area']
    Context.used_columns.update(cols)
    df = df[cols].drop_duplicates().reset_index(drop=True)
    df.columns = ['crop_name', 'harvest_year', 'crop_area', 'uom_crop_area']
    df.insert(0, 'crop_id', df.index + 1)
    return df
