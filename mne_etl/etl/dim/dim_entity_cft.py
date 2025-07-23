from etl.utils.model_builder import build_model_safe

def build_dim_entity_cft(df):
    keywords = {
        'entity_id': ['Entity ID', 'EntitySystemID'],
        'entity_name': ['EntityName'],
        'interviewee': ['Interviewee', 'Respondent']
    }
    return build_model_safe(df, keywords, label='dim_entity_cft')
