import pandas as pd
from etl.utils.matcher import match

def build_dim_farmer(df: pd.DataFrame) -> pd.DataFrame:
    # Define semantic mapping
    keywords = {
        'entity_id': ['Entity ID'],
        'first_name': ['First Name'],
        'last_name': ['Last Name'],
        'phone': ['Phone Number'],
        'email': ['Email'],
        'education_level': ['Education Level'],
    }

    # Match columns dynamically
    matched = {k: match(df, v) for k, v in keywords.items()}
    missing = [k for k, v in matched.items() if v is None]
    found = {k: v for k, v in matched.items() if v is not None}

    if missing:
        print(f"⚠️ Missing in dim_farmer → {missing}")
    if "entity_id" not in found:
        print("❌ 'entity_id' not found. Returning empty DataFrame.")
        return df.iloc[0:0].copy()

    # Create subset and rename columns
    out = df[list(found.values())].copy().rename(columns={v: k for k, v in found.items()})
    
    # Drop rows with only entity_id and all other fields as null
    other_fields = [k for k in out.columns if k != 'entity_id']
    out = out[~out[other_fields].isnull().all(axis=1)]

    return out.drop_duplicates().reset_index(drop=True)
