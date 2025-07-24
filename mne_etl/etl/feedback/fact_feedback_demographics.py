import pandas as pd
from difflib import SequenceMatcher
from etl.utils.matcher import match

def fuzzy_match_column(df: pd.DataFrame, aliases: list, threshold: float = 0.85) -> str:
    best_col = None
    best_score = 0

    for alias in aliases:
        for col in df.columns:
            score = SequenceMatcher(None, alias.lower(), col.lower()).ratio()
            if score > best_score and score >= threshold:
                best_col = col
                best_score = score
    return best_col

def robust_match(df: pd.DataFrame, aliases: list) -> str:
    col = match(df, aliases)
    if col is None:
        col = fuzzy_match_column(df, aliases)
    return col

def build_fact_feedback_demographics(df: pd.DataFrame, dim_education_df: pd.DataFrame) -> pd.DataFrame:
    # 🔍 Semantic mappings with flexible phrasing
    keywords = {
        'entity_id': ['Entity ID'],
        'successor': [
            'Successor',
            'Who will take over the farm',
            'Who is the successor to your farm?',
            'Successor to the farm',
            'Farm successor'
        ],
        'education_level': ['Education Level', 'Highest education attained'],
        'num_adults': ['Number of adults', 'Adults in household', 'Number of adult family members'],
        'num_boys': ['Number of boys', 'Number of male children'],
        'num_girls': ['Number of girls', 'Number of female children'],
        'collection_date': ['Date of the Data Collection', 'Date of Collection'],
        'survey_year': ['Year of reporting', 'Survey Year', 'Reporting Year']
    }

    # 🤖 Match columns using hybrid matcher
    matched_columns = {k: robust_match(df, v) for k, v in keywords.items()}
    missing = [k for k, v in matched_columns.items() if v is None]
    found = {k: v for k, v in matched_columns.items() if v is not None}

    if missing:
        print(f"⚠️ Missing in fact_feedback_demographics → {missing}")
    if not found:
        print("❌ No columns matched. Returning empty DataFrame.")
        return df.iloc[0:0].copy()

    # 📦 Extract and rename matched columns
    out = df[list(found.values())].copy()
    out.columns = list(found.keys())

    # 🔗 Normalize education_level and attach education_id
    if 'education_level' in out.columns and 'education_level' in dim_education_df.columns:
        out = out.merge(dim_education_df, on='education_level', how='left')

    # 🧹 Deduplicate by entity-year
    out = out.drop_duplicates(subset=['entity_id', 'survey_year']).reset_index(drop=True)

    # ✂️ Final field selection
    final_cols = ['entity_id', 'survey_year', 'collection_date', 'num_adults',
                  'num_boys', 'num_girls', 'successor', 'education_id']
    available_cols = [col for col in final_cols if col in out.columns]

    return out[available_cols].copy()
