import pandas as pd
from etl.context import Context
from etl.utils.matcher import match

def build_fact_co_product(df: pd.DataFrame, entity_lookup: dict) -> pd.DataFrame:
    # 🔍 Match columns with semantic aliases
    keywords = {
        'result_id': ['Result ID'],
        'result_id_parent': ['Result ID Parent'],
        'co_product_uid': ['Co-Product UID', 'CoProduct UID', 'Co Product ID'],
        'entity_system_id': ['EntitySystemID', 'System ID', 'Entity ID'],
        'type': ['Type of co-product', 'Co-Product Type'],
        'value_relative_to_crop': ['Value relative to crop (%)', 'Relative Value %'],
        'country': ['Country'],
        'business_name': ['BusinessName', 'Business Name'],
        'date_created': ['DateCreated', 'Created Date'],
        'date_synched': ['DateSynched', 'Date Synched'],
        'fa_name': ['FAName', 'Field Agent', 'Facilitator Name'],
        'data_versioning': ['Data Versioning', 'Version'],
        'last_modified_by': ['LastModified by', 'Modified By'],
        'last_updated': ['Last Updated', 'Updated Date']
    }

    # 🧠 Apply matcher to columns
    matched = {k: match(df, v) for k, v in keywords.items()}
    missing = [k for k, v in matched.items() if v is None]
    found = {k: v for k, v in matched.items() if v is not None}

    if missing:
        print(f"⚠️ Missing in fact_co_product → {missing}")
    if not found:
        print("❌ No columns matched. Returning empty DataFrame.")
        return df.iloc[0:0].copy()

    Context.used_columns.update(found.values())

    # 🎛 Clean and rename
    df = df[list(found.values())].copy().rename(columns={v: k for k, v in found.items()})

    # 🔗 Map entity_id
    if 'entity_system_id' in df.columns:
        df['entity_id'] = df['entity_system_id'].map(entity_lookup)

    # 🆔 Add surrogate key
    df.insert(0, 'co_product_id', range(1, len(df) + 1))

    # 🚫 Drop records with only co_product_id and all other columns null
    non_id_cols = [col for col in df.columns if col not in {'co_product_id', 'entity_id'}]
    df = df[~df[non_id_cols].isnull().all(axis=1)]

    return df.drop_duplicates().reset_index(drop=True)
