from etl.utils.matcher import match
import pandas as pd

def build_fact_agro_inputs(df: pd.DataFrame) -> pd.DataFrame:
    # Semantic keyword mapping
    keywords = {
        'entity_id': ['Entity ID'],
        'fertilizer_used': ['most frequently used fertilizer'],
        'n_pct': ['n%'],
        'p_pct': ['p%'],
        'k_pct': ['k%'],
        'cash_incentive': ['cash incentive'],
        'unacceptable_4c': ['4c unacceptable'],
        'herbicide_usage': ['herbicide usage'],
        'weed_management': ['weed management']
    }

    # Match columns dynamically
    matched = {k: match(df, v) for k, v in keywords.items()}
    missing = [k for k, v in matched.items() if v is None]
    found = {k: v for k, v in matched.items() if v is not None}

    if missing:
        print(f"⚠️ Missing in fact_agro_inputs → {missing}")

    if not found:
        print("❌ No valid columns found for fact_agro_inputs. Returning empty DataFrame.")
        return df.iloc[0:0].copy()

    # Extract and rename valid columns
    out = df[list(found.values())].copy()
    out = out.rename(columns={v: k for k, v in found.items()})
    return out.dropna(how="all").drop_duplicates().reset_index(drop=True)
