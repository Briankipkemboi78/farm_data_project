import pandas as pd
from etl.context import Context

def build_dim_fields(df: pd.DataFrame) -> pd.DataFrame:
    cols = ['Your field name', 'Specialization', 'Size', 'Intensity',
            'Has any part of this field been converted in the last 20 years']
    Context.used_columns.update(cols)
    df = df[cols].drop_duplicates().reset_index(drop=True)
    df.columns = ['field_name', 'specialization', 'size', 'intensity', 'converted_flag']
    df.insert(0, 'field_id', df.index + 1)
    return df
