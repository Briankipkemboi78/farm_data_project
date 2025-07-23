from etl.utils.matcher import match
import pandas as pd

def build_fact_soil_assessment(df: pd.DataFrame) -> pd.DataFrame:
    # Define semantic keywords
    keywords = {
        'entity_id': ['Entity ID'],
        'cover_crops': ['cover crops'],
        'erosion_control': ['erosion control'],
        'soil_analysis': ['soil analysis'],
        'interval_soil_analysis': ['interval of soil analysis'],
        'fertilizer_plan': ['fertilizer plan'],
        'soil_organic_matter': ['soil organic matter'],
        'soil_ph': ['Soil pH']
    }

    # Match columns dynamically
    columns = {k: match(df, v) for k, v in keywords.items()}
    missing = [k for k, v in columns.items() if v is None]

    if missing:
        print(f"⚠️ Warning: Missing columns in fact_soil_assessment: {missing}")

    valid_columns = {k: v for k, v in columns.items() if v is not None}

    if not valid_columns:
        print("❌ No valid columns found for fact_soil_assessment. Returning empty DataFrame.")
        return df.iloc[0:0].copy()

    # Build cleaned output
    out = df[list(valid_columns.values())].copy()
    out = out.rename(columns={v: k for k, v in valid_columns.items()})
    return out.drop_duplicates().reset_index(drop=True)
