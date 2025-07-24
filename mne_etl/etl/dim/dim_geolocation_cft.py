import pandas as pd
from etl.context import Context
from etl.utils.matcher import match

def build_dim_geolocation(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Match separate latitude and longitude columns
    lat_col = match(df, ['Latitude', 'GPS Latitude', 'Lat'])
    lon_col = match(df, ['Longitude', 'GPS Longitude', 'Long', 'Lng'])

    if lat_col and lon_col:
        Context.used_columns.update([lat_col, lon_col])
        df['raw_location'] = 'POINT(' + df[lon_col].astype(str) + ' ' + df[lat_col].astype(str) + ')'
        geo_df = df[['raw_location', lat_col, lon_col]].drop_duplicates()
        geo_df.columns = ['raw_location', 'latitude', 'longitude']
    else:
        # Fall back to combined GPS column
        gps_col = match(df, ['GPS Location', 'GPS', 'Geolocation', 'GPS Coordinates'])
        if gps_col is None:
            print("❌ Latitude and/or Longitude columns not found. Returning empty DataFrame.")
            return df.iloc[0:0].copy()

        Context.used_columns.update([gps_col])
        df['latitude'] = df[gps_col].str.extract(r'POINT\(([-\d\.]+) [-\d\.]+\)')
        df['longitude'] = df[gps_col].str.extract(r'POINT\([-.\d]+ ([-\d\.]+)\)')
        geo_df = df[[gps_col, 'latitude', 'longitude']].drop_duplicates()
        geo_df.columns = ['raw_location', 'latitude', 'longitude']

    # Add unique identifier
    geo_df.insert(0, 'location_id', range(1, len(geo_df) + 1))

    # 🚫 Drop records with missing coordinates
    geo_df = geo_df.dropna(subset=['latitude', 'longitude'], how='any')
    
    return geo_df.reset_index(drop=True)
