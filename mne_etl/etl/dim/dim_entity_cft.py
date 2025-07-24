from etl.utils.matcher import match

def build_dim_entity_cft(df):
    cols = {
        'entity_id': match(df, ['EntitySystemID']),
        'entity_name': match(df, ['EntityName']),
        'interviewee': match(df, ['Interviewee', 'Respondent'])
    }
    out = df[list(cols.values())].copy().rename(columns={v: k for k, v in cols.items()})
    return out.drop_duplicates().reset_index(drop=True)
