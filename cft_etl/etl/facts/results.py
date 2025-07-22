import pandas as pd
from etl.context import Context

def build_fact_results(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        'result_id', 'status', 'Approval Status', 'Remarks',
        'Detail Incomplete', 'Last sent date API', 'CFT API Version',
        'Last Updated', 'LastModified by'
    ]
    Context.used_columns.update(cols)
    df = df[cols].drop_duplicates().reset_index(drop=True)
    df.columns = [
        'result_id', 'status', 'approval_status', 'remarks',
        'incomplete_flag', 'last_sent_date', 'api_version',
        'last_updated', 'last_modified_by'
    ]
    return df
