from etl.utils.matcher import match
import pandas as pd

def build_fact_recordkeeping(df: pd.DataFrame) -> pd.DataFrame:
    keywords = {
        'entity_id': ['Entity ID'],
        'financial_records': ['financial management records'],
        'cash_incentive': ['cash incentive'],
        'insurance': ['insurance'],
        'vsla': ['VSLA']
    }

    matched = {k: match(df, v) for k, v in keywords.items()}
    missing = [k for k, v in matched.items() if v is None]
    found = {k: v for k, v in matched.items() if v is not None}

    if missing:
        print(f"⚠️ Missing in fact_recordkeeping → {missing}")

    if not found:
        print("❌ No valid columns found for fact_recordkeeping. Returning empty DataFrame.")
        return df.iloc[0:0].copy()

    out = df[list(found.values())].copy()
    out = out.rename(columns={v: k for k, v in found.items()})
    return out.dropna(how="all").drop_duplicates().reset_index(drop=True)
