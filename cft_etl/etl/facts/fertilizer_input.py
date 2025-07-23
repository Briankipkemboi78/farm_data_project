import pandas as pd
from etl.context import Context

def build_fact_fertilizer_input(df: pd.DataFrame, entity_lookup: dict) -> pd.DataFrame:
    expected_cols = [
        'result_id', 'result_id_parent', 'Fertilizer Input UID', 'EntitySystemID',
        'Fertilizer type', 
        '% N (as ammonium-N)', '& N (as nitrate - N)', '% N (as urea - N)', '(%) N',
        '% P2O5 or % P', 'UOM P205 or P', '% K2O or % K', 'UOM K20 or K',
        'Total ingredients', 'Application rate', 'UOM Application rate',
        'Fertilizer weight or units', 'Application method',
        'Manufactured in', 'FAName', 'Country', 'BusinessName',
        'DateCreated', 'DateSynched', 'Data Versioning',
        'LastModified by', 'Last Updated'
    ]

    available_cols = df.columns.tolist()
    missing_cols = set(expected_cols) - set(available_cols)

    if missing_cols:
        raise KeyError(f"Missing columns in Fertilizer Input sheet: {missing_cols}")

    Context.used_columns.update(expected_cols)

    df = df[expected_cols].drop_duplicates().reset_index(drop=True)

    df.columns = [
        'result_id', 'result_id_parent', 'fertilizer_input_uid', 'entity_system_id',
        'fertilizer_type',
        'n_ammonium', 'n_nitrate', 'n_urea', 'n_total',
        'p_content', 'uom_p', 'k_content', 'uom_k',
        'total_ingredients', 'application_rate', 'uom_application_rate',
        'fertilizer_unit', 'application_method',
        'manufactured_in', 'fa_name', 'country', 'business_name',
        'date_created', 'date_synched', 'data_versioning',
        'last_modified_by', 'last_updated'
    ]

    df['entity_id'] = df['entity_system_id'].map(entity_lookup)
    df.insert(0, 'fertilizer_input_id', df.index + 1)

    return df
