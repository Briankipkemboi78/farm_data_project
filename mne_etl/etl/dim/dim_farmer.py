def build_dim_farmer(df):
    from etl.utils.matcher import match
    keys = {
        'entity_id': match(df, ['Entity ID']),
        'first_name': match(df, ['First Name']),
        'last_name': match(df, ['Last Name']),
        'phone': match(df, ['Phone Number']),
        'email': match(df, ['Email']),
        'identification_id': match(df, ['Identification ID']),
        'identification_type': match(df, ['Identification ID Type']),
        'education_level': match(df, ['Education Level']),
    }
    out = df[list(keys.values())].copy().rename(columns={v: k for k, v in keys.items()})
    return out.drop_duplicates().reset_index(drop=True)
