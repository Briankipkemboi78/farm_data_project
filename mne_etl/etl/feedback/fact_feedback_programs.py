import pandas as pd
from etl.utils.matcher import match

def build_fact_feedback_programs(df):

    keywords = ['nescafe', 'technical visits', 'training satisfaction', 'plantlets', 'renovation']
    fields = [col for col in df.columns if any(k.lower() in col.lower() for k in keywords)]

    out = pd.melt(df, id_vars=[match(df, ['Entity ID'])], value_vars=fields)
    out.columns = ['entity_id', 'question_label', 'response_text']
    out['theme_label'] = 'programs'
    out['feedback_id'] = range(1, len(out)+1)
    return out.dropna().drop_duplicates().reset_index(drop=True)
