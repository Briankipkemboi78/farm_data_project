from etl.utils.matcher import match

def build_dim_location(df):
    keys = {
        'entity_id': match(df, ['Entity ID']),
        'country': match(df, ['Country']),
        'region': match(df, ['Region']),
        'sub_region': match(df, ['Sub Region']),
        'unit': match(df, ['Unit']),
        'latitude': match(df, ['Latitude']),
        'longitude': match(df, ['Longitude']),
        'altitude': match(df, ['Altitude']),
        'address_1': match(df, ['Address 1']),
        'address_2': match(df, ['Address 2']),
    }

    # ✅ Filter out missing columns
    valid_keys = {k: v for k, v in keys.items() if v is not None}
    missing = [k for k, v in keys.items() if v is None]

    if missing:
        print(f"⚠️ Warning: The following columns were not found in dim_location: {missing}")

    if not valid_keys:
        print("❌ No valid columns matched for dim_location. Returning empty DataFrame.")
        return df.iloc[0:0].copy()  # Empty frame with original schema

    out = df[list(valid_keys.values())].copy()
    out = out.rename(columns={v: k for k, v in valid_keys.items()})
    return out.drop_duplicates().reset_index(drop=True)

