import pandas as pd
from etl.context import Context

def build_fact_audit(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        'result_id',
        'Approval Status'
    ]
    Context.used_columns.update(cols)

    df = df[cols].drop_duplicates().reset_index(drop=True)
    df.columns = [
        'result_id',
        'approval_status'
    ]
    df.insert(0, 'audit_id', df.index + 1)
    return df
