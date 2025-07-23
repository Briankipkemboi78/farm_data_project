from etl.utils.matcher import match
import pandas as pd

def build_fact_nescafe_plan(df: pd.DataFrame) -> pd.DataFrame:
    keywords = {
        'entity_id': ['Entity ID'],
        'year_joined_nescafe_plan': ['Year joined Nescafe Plan'],
        'training_sessions_male': ['training sessions male'],
        'training_sessions_female': ['training sessions female'],
        'training_sessions_youth': ['training sessions youth'],
        'technical_visits': ['technical visits'],
        'plantlets_received': ['plantlets received'],
        'plantlets_survived': ['plantlets survived'],
        'condition_plantlets': ['condition of plantlets'],
        'satisfaction_plantlets': ['satisfaction with plantlets'],
        'renovation': ['renovation'],
        'expansion': ['expansion']
    }

    matched = {k: match(df, v) for k, v in keywords.items()}
    missing = [k for k, v in matched.items() if v is None]
    found = {k: v for k, v in matched.items() if v is not None}

    if missing:
        print(f"⚠️ Missing in fact_nescafe_plan → {missing}")

    if not found:
        print("❌ No valid columns found for fact_nescafe_plan. Returning empty DataFrame.")
        return df.iloc[0:0].copy()

    out = df[list(found.values())].copy()
    out = out.rename(columns={v: k for k, v in found.items()})
    return out.dropna(how="all").drop_duplicates().reset_index(drop=True)
