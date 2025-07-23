from etl.utils.matcher import match
import pandas as pd

def build_fact_water_management(df: pd.DataFrame) -> pd.DataFrame:
    # Define semantic keyword mapping
    keywords = {
        'entity_id': ['Entity ID'],
        'irrigate_coffee': ['irrigate your coffee'],
        'irrigation_source': ['irrigation water source'],
        'irrigation_rounds': ['irrigation rounds/year'],
        'irrigation_quantity': ['irrigation water per round'],
        'wet_processing': ['wet processing'],
        'soil_moisture_monitoring': ['soil moisture monitoring'],
        'wastewater_treatment': ['wastewater treatment'],
        'distance_to_water': ['distance between field and water body'],
        'riparian_buffer': ['riparian buffer strips']
    }

    # Match each column dynamically
    matched_columns = {k: match(df, v) for k, v in keywords.items()}
    missing = [k for k, v in matched_columns.items() if v is None]
    found = {k: v for k, v in matched_columns.items() if v is not None}

    if missing:
        print(f"⚠️ Missing in fact_water_management → {missing}")

    if not found:
        print("❌ No columns matched for fact_water_management. Returning empty DataFrame.")
        return df.iloc[0:0].copy()

    out = df[list(found.values())].copy()
    out = out.rename(columns={v: k for k, v in found.items()})
    return out.dropna(how="all").drop_duplicates().reset_index(drop=True)
