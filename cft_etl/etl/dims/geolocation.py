import pandas as pd
from etl.context import Context

def build_dim_geolocation(df: pd.DataFrame) -> pd.DataFrame:
    used_cols = ['GPS Location']
    Context.used_columns.update(used_cols)
    df = df.copy()
    df['latitude'] = df['GPS Location'].str.extract(r'POINT\(([-\d\.]+) [-\d\.]+\)')
    df['longitude'] = df['GPS Location'].str.extract(r'POINT\([-.\d]+ ([-\d\.]+)\)')
    geo_df = df[['GPS Location', 'latitude', 'longitude']].drop_duplicates()
    geo_df.columns = ['raw_location', 'latitude', 'longitude']
    geo_df.insert(0, 'location_id', geo_df.index + 1)
    return geo_df.reset_index(drop=True)
