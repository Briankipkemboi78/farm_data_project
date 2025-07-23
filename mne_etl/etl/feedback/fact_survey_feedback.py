
import pandas as pd 
from etl.utils.matcher import match


def build_fact_survey_feedback(df, used_columns):
  

    # Identify object-type columns not already used
    unmapped = [col for col in df.columns if col not in used_columns and df[col].dtype == 'object']
    fields = [col for col in unmapped if df[col].nunique(dropna=True) > 10]

    out = pd.melt(df, id_vars=[match(df, ['Entity ID'])], value_vars=fields)
    out.columns = ['entity_id', 'question_label', 'response_text']
    out['feedback_id'] = range(1, len(out)+1)
    return out.dropna().drop_duplicates().reset_index(drop=True)
