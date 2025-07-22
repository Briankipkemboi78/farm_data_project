import pandas as pd
from etl.context import Context

def build_fact_co_product(df: pd.DataFrame, entity_lookup: dict) -> pd.DataFrame:
    cols = [
        'result_id', 'result_id_parent', 'Co-Product UID', 'EntitySystemID',
        'Type of co-product', 'Value relative to crop (%)',
        'Country', 'BusinessName', 'DateCreated', 'DateSynched',
        'FAName', 'Data Versioning', 'LastModified by', 'Last Updated'
    ]
    Context.used_columns.update(cols)
    df = df[cols].drop_duplicates().reset_index(drop=True)

    df.columns = [
        'result_id', 'result_id_parent', 'co_product_uid', 'entity_system_id',
        'type', 'value_relative_to_crop',
        'country', 'business_name', 'date_created', 'date_synched',
        'fa_name', 'data_versioning', 'last_modified_by', 'last_updated'
    ]

    # Map entity_id
    df['entity_id'] = df['entity_system_id'].map(entity_lookup)

    # Insert surrogate key
    df.insert(0, 'co_product_id', df.index + 1)

    return df
