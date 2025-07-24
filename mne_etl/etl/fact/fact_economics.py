from etl.utils.matcher import match
import pandas as pd

def build_fact_economics(df: pd.DataFrame) -> pd.DataFrame:
    # Define semantic mapping
    keywords = {
        'entity_id': ['Entity ID'],
        'cash_crop_1': ['1st main cash crop'],
        'cash_crop_2': ['2nd main cash crop'],
        'other_crop_1': ['Other crop 1'],
        'other_crop_2': ['Other crop 2'],
        'livestock': ['Livestock'],
        'coffee_sales': ['sell your coffee']
    }

    # Match columns dynamically
    matched_columns = {k: match(df, v) for k, v in keywords.items()}
    missing = [k for k, v in matched_columns.items() if v is None]
    found = {k: v for k, v in matched_columns.items() if v is not None}

    if missing:
        print(f"⚠️ Missing in fact_economics → {missing}")
    if not found:
        print("❌ No columns matched. Returning empty DataFrame.")
        return df.iloc[0:0].copy()

    # Extract matched columns and normalize names
    selected_columns = list(found.values())
    out = df[selected_columns].copy()
    out.columns = [col.lower().replace(" ", "_") for col in out.columns]

    return out.drop_duplicates().reset_index(drop=True)
