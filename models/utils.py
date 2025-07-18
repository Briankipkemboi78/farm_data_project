import pandas as pd
import re
import os

def clean_entity_id(df: pd.DataFrame, column_name: str = "Entity ID") -> pd.DataFrame:
    """
    Ensures entity ID is treated as string and strips leading apostrophes.
    """
    df[column_name] = df[column_name].astype(str).str.lstrip("'")
    return df


def deduplicate_columns(df: pd.DataFrame, output_log_path: str = "output/column_deduplication_log.csv") -> pd.DataFrame:
    """
    Deduplicates columns by:
    - Grouping columns with same base name (removing .1, .2, etc.)
    - Prioritizing columns with '(Update)' in the name
    - If no update column, keep the one with most non-null values
    - Logs which columns were retained vs dropped
    """
    col_groups = {}
    dedup_log = []

    for col in df.columns:
        base = re.sub(r'\.\d+$', '', col)
        base = re.sub(r'\s*\(update\)', '', base, flags=re.IGNORECASE).strip()
        col_groups.setdefault(base, []).append(col)

    cleaned_df = pd.DataFrame()

    for base, variants in col_groups.items():
        if len(variants) == 1:
            best_col = variants[0]
        else:
            update_cols = [col for col in variants if "(update)" in col.lower()]
            if update_cols:
                best_col = update_cols[0]
            else:
                best_col = df[variants].notna().sum().idxmax()

        # Save best column
        cleaned_df[base] = df[best_col]

        for col in variants:
            dedup_log.append({
                "base_column": base,
                "original_column": col,
                "kept": col == best_col
            })

    os.makedirs(os.path.dirname(output_log_path), exist_ok=True)
    pd.DataFrame(dedup_log).to_csv(output_log_path, index=False)
    pd.DataFrame({"cleaned_column_name": cleaned_df.columns}).to_csv("output/cleaned_columns.csv", index=False)

    return cleaned_df


def drop_empty_columns(df: pd.DataFrame, log_path: str = "output/null_columns_dropped.csv") -> pd.DataFrame:
    """
    Drops columns that are entirely null, empty string, dash ("-"), or whitespace.
    Saves a log of dropped and retained columns.
    """
    null_like = ["", "-", " "]
    is_empty = df.apply(lambda col: col.astype(str).str.strip().isin(null_like) | col.isna())
    all_empty_cols = is_empty.all(axis=0)
    
    dropped_cols = df.columns[all_empty_cols].tolist()
    retained_cols = df.columns[~all_empty_cols].tolist()

    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    # Save dropped columns
    pd.DataFrame({"dropped_column": dropped_cols}).to_csv(log_path, index=False)

    # Save retained columns
    pd.DataFrame({"retained_column": retained_cols}).to_csv("output/retained_columns.csv", index=False)

    return df.drop(columns=dropped_cols)
