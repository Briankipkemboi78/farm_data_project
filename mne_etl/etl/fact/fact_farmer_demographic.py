from etl.utils.matcher import match
import pandas as pd

def build_fact_feedback_demographics(df: pd.DataFrame) -> pd.DataFrame:
    # Semantic keyword mapping
    keywords = {
        'entity_id': ['Entity ID'],
        'successor': ['successor'],
        'education_level': ['education level', 'highest education attained'],
        'num_adults': ['number of adults'],
        'num_boys': ['number of boys'],
        'num_girls': ['number of girls']
    }

    # Match columns dynamically
    matched_columns = {k: match(df, v) for k, v in keywords.items()}
    missing = [k for k, v in matched_columns.items() if v is None]
    found = {k: v for k, v in matched_columns.items() if v is not None}

    if missing:
        print(f"⚠️ Missing in fact_feedback_demographics → {missing}")
    if not found:
        print("❌ No columns matched. Returning empty DataFrame.")
        return df.iloc[0:0].copy()

    # Reshape into feedback format
    fields = [v for k, v in found.items() if k != "entity_id"]
    entity_col = found["entity_id"]

    out = pd.melt(df, id_vars=[entity_col], value_vars=fields)
    out.columns = ['entity_id', 'question_label', 'response_text']
    out['theme_label'] = 'demographics'
    out['feedback_id'] = range(1, len(out) + 1)

    return out.dropna(how="any").drop_duplicates().reset_index(drop=True)
