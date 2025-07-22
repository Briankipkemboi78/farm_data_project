import pandas as pd
from etl.context import Context

def build_fact_energy_usage(df: pd.DataFrame, entity_lookup: dict) -> pd.DataFrame:
    cols = [
        'result_id', 'result_id_parent', 'Energy Usage UID', 'EntitySystemID',
        'Source', 'Energy usage', 'UOM Energy usage',
        'Category', 'Label', 'FA Name',
        'DateCreated', 'DateSynched', 'Last Updated',
        'LastModified by', 'Data Versioning'
    ]
    Context.used_columns.update(cols)
    df = df[cols].drop_duplicates().reset_index(drop=True)

    df.columns = [
        'result_id', 'result_id_parent', 'energy_usage_uid', 'entity_system_id',
        'source_type', 'usage_quantity', 'uom_usage_quantity',
        'category', 'label', 'fa_name',
        'date_created', 'date_synched', 'last_updated',
        'last_modified_by', 'data_versioning'
    ]

    df['entity_id'] = df['entity_system_id'].map(entity_lookup)
    df.insert(0, 'energy_usage_id', df.index + 1)

    return df
