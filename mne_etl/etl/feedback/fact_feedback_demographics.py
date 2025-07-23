import pandas as pd
from etl.utils.matcher import match

def build_fact_feedback_demographics(df: pd.DataFrame, dim_education_df: pd.DataFrame) -> pd.DataFrame:
    keywords = {
        'entity_id': ['Entity ID'],
        'successor': ['Successor', 'Who will take over the farm'],
        'education_level': ['Education Level', 'Highest education attained'],
        'num_adults': ['Number of adults', 'number of adults in farmers family'],
        'num_boys': ['Number of boys'],
        'num_girls': ['Number of girls'],
        'collection_date': ['Date of the Data Collection', 'Date of Collection'],
        'survey_year': ['Year of reporting', 'Survey Year', 'Reporting Year']
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

    # Extract and rename available columns
    out = df[list(found.values())].copy()
    out.columns = list(found.keys())

    # Normalize education_level and attach education_id
    if 'education_level' in out.columns:
        out = out.merge(dim_education_df, on='education_level', how='left')

    # Ensure 1 record per entity-year (drop duplicates)
    out = out.drop_duplicates(subset=['entity_id', 'survey_year']).reset_index(drop=True)

    # Safe return — include only available columns from this desired set
    final_cols = ['entity_id', 'survey_year', 'collection_date', 'num_adults',
                  'num_boys', 'num_girls', 'successor', 'education_id']
    available_cols = [col for col in final_cols if col in out.columns]

    return out[available_cols].copy()
