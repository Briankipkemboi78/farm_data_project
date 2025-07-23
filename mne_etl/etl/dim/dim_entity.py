def build_dim_entity(df):
    from etl.utils.matcher import match
    cols = {
        'entity_id': match(df, ['Entity ID']),
        'entity_name': match(df, ['Entity Name']),
        'farm_id': match(df, ['Farm ID']),
        'gender': match(df, ['Gender']),
        'year_of_birth': match(df, ['Year of Birth']),
        'relation_to_entity': match(df, ['Relation to Entity']),
        'lead_farmer': match(df, ['Lead Farmer']),
        'local_group': match(df, ['Local Group'])
    }
    out = df[list(cols.values())].copy().rename(columns={v: k for k, v in cols.items()})
    return out.drop_duplicates().reset_index(drop=True)
