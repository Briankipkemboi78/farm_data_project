def build_dim_identification(df):
    from etl.utils.matcher import match
    keys = {
        'entity_id': match(df, ['Entity ID']),
        'identification_id': match(df, ['Identification ID']),
        'identification_type': match(df, ['Identification ID Type']),
    }
    out = df[list(keys.values())].copy().rename(columns={v: k for k, v in keys.items()})
    return out.dropna().drop_duplicates().reset_index(drop=True)
