import pandas as pd
import re
import os
import unicodedata

def clean_entity_id(df: pd.DataFrame, column_name: str = "Entity ID") -> pd.DataFrame:
    df[column_name] = df[column_name].astype(str).str.lstrip("'")
    return df

def normalize_column_name(col: str) -> str:
    col = re.sub(r'[“”"‘’\'].*?[“”"‘’\']', '', col)
    col = col.replace('Â', '').replace('�', '')
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('utf-8')
    col = re.sub(r'[^\x20-\x7E]', '', col)
    col = re.sub(r'\s+', ' ', col)
    return col.strip()

import pandas as pd
import os
import re

def deduplicate_columns(df: pd.DataFrame, output_log_path: str = "output/column_deduplication_log.csv") -> pd.DataFrame:
    col_groups = {}
    dedup_log = []
    cleaned_columns = {}

    # Group columns by base name
    for col in df.columns:
        base = re.sub(r'\.\d+$', '', col)
        base = re.sub(r'\s*\(update\)', '', base, flags=re.IGNORECASE).strip()
        col_groups.setdefault(base, []).append(col)

    for base, variants in col_groups.items():
        if len(variants) == 1:
            cleaned_columns[base] = df[variants[0]]
            dedup_log.append({"base_column": base, "original_column": variants[0], "kept": True})
        else:
            # Combine all columns by priority
            combined = pd.Series([pd.NA] * len(df), index=df.index)
            for col in variants:
                combined = combined.combine_first(df[col])
            cleaned_columns[base] = combined

            # Logging
            most_used = pd.DataFrame({col: df[col].notna().sum() for col in variants}, index=["non_nulls"]).T
            best_col = most_used["non_nulls"].idxmax()
            for col in variants:
                dedup_log.append({
                    "base_column": base,
                    "original_column": col,
                    "kept": col == best_col  # Mark the most populated column as 'kept' for logging purposes
                })

    cleaned_df = pd.DataFrame(cleaned_columns)
    cleaned_df.columns.name = None

    os.makedirs(os.path.dirname(output_log_path), exist_ok=True)
    pd.DataFrame(dedup_log).to_csv(output_log_path, index=False)
    pd.DataFrame({"cleaned_column_name": cleaned_df.columns}).to_csv("output/cleaned_columns.csv", index=False)

    return cleaned_df





def drop_empty_columns(df: pd.DataFrame, log_path: str = "output/null_columns_dropped.csv") -> pd.DataFrame:
    null_like = ["", "-", " "]
    is_empty = df.apply(lambda col: col.astype(str).str.strip().isin(null_like) | col.isna())
    all_empty_cols = is_empty.all(axis=0)

    dropped_cols = df.columns[all_empty_cols].tolist()
    retained_cols = df.columns[~all_empty_cols].tolist()

    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    pd.DataFrame({"dropped_column": dropped_cols}).to_csv(log_path, index=False)
    pd.DataFrame({"retained_column": retained_cols}).to_csv("output/retained_columns.csv", index=False)

    return df.drop(columns=dropped_cols)

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    cleaned_cols = [normalize_column_name(col) for col in df.columns]
    df.columns = cleaned_cols
    return df

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_empty_columns(df)
    df = clean_column_names(df)
    df = deduplicate_columns(df)
    df = clean_entity_id(df)
    return df
