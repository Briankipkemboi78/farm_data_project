from etl.utils.matcher import match
import pandas as pd

def build_model_safe(df: pd.DataFrame, keywords: dict, label: str = None) -> pd.DataFrame:
    matched = {k: match(df, v) for k, v in keywords.items()}
    found = {k: v for k, v in matched.items() if v is not None}
    missing = [k for k in keywords if k not in found]

    if label and missing:
        print(f"⚠️ Missing columns in {label}: {missing}")
    if not found:
        print(f"❌ {label} has no matched columns. Returning empty DataFrame.")
        return df.iloc[0:0].copy()

    out = df[list(found.values())].copy()
    out = out.rename(columns={v: k for k, v in found.items()})
    return out.drop_duplicates().reset_index(drop=True)
