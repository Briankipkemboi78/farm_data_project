import pandas as pd
from etl.utils.matcher import match

def build_fact_energy_usage(df: pd.DataFrame, entity_lookup: dict) -> pd.DataFrame:
    # 🧠 Define semantic column aliases
    keywords = {
        'result_id': ['Result ID'],
        'result_id_parent': ['Result ID Parent'],
        'energy_usage_uid': ['Energy Usage UID', 'Energy UID'],
        'entity_system_id': ['EntitySystemID', 'System ID'],
        'source_type': ['Source', 'Source Type'],
        'usage_quantity': ['Energy usage', 'Usage Amount'],
        'uom_usage_quantity': ['UOM Energy usage', 'Unit of Measurement'],
        'category': ['Category'],
        'label': ['Label'],
        'fa_name': ['FA Name', 'Field Agent Name'],
        'date_created': ['DateCreated', 'Created Date'],
        'date_synched': ['DateSynched', 'Date Synched'],
        'last_updated': ['Last Updated'],
        'last_modified_by': ['LastModified by', 'Modified By'],
        'data_versioning': ['Data Versioning', 'Versioning']
    }

    # 🔍 Match columns dynamically
    matched_columns = {k: match(df, v) for k, v in keywords.items()}
    missing = [k for k, v in matched_columns.items() if v is None]
    found = {k: v for k, v in matched_columns.items() if v is not None}

    if missing:
        print(f"⚠️ Missing in fact_energy_usage → {missing}")
    if not found:
        print("❌ No columns matched. Returning empty DataFrame.")
        return df.iloc[0:0].copy()

    # 🧱 Extract and rename columns
    df = df[list(found.values())].copy().rename(columns={v: k for k, v in found.items()})

    # 🔗 Map entity_id from system ID
    if 'entity_system_id' in df.columns:
        df['entity_id'] = df['entity_system_id'].map(entity_lookup)

    # 🆔 Insert energy_usage_id as primary key
    df.insert(0, 'energy_usage_id', range(1, len(df) + 1))

    return df.drop_duplicates().reset_index(drop=True)
