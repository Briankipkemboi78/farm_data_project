import pandas as pd
from etl.utils.matcher import match

def build_fact_survey_feedback(df: pd.DataFrame, dim_education_df: pd.DataFrame, used_columns: list) -> pd.DataFrame:
    # Identify object-type columns not already used
    unmapped = [col for col in df.columns if col not in used_columns and df[col].dtype == 'object']
    fields = [col for col in unmapped if df[col].nunique(dropna=True) > 10]

    # Semantic match for entity_id
    entity_col = match(df, ['Entity ID'])
    if not entity_col:
        print("❌ 'Entity ID' not found in dataset")
        return df.iloc[0:0].copy()

    # Reshape into long format
    out = pd.melt(df, id_vars=[entity_col], value_vars=fields)
    out.columns = ['entity_id', 'question_label', 'response_text']

    # Attach education_id if response_text matches education_level
    if 'education_level' in dim_education_df.columns:
        out = out.merge(dim_education_df, left_on='response_text', right_on='education_level', how='left')
    else:
        print("⚠️ 'education_level' column missing in dim_education_df — skipping merge")

    # Metadata for context
    out['theme_label'] = 'general_feedback'
    out['feedback_id'] = range(1, len(out) + 1)

    return out.dropna(how="any").drop_duplicates().reset_index(drop=True)
