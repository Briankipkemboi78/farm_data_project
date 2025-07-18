import pandas as pd
import re
import os
import unicodedata
import string

def clean_entity_id(df: pd.DataFrame, column_name: str = "Entity ID") -> pd.DataFrame:
    df[column_name] = df[column_name].astype(str).str.lstrip("'")
    return df

def normalize_column_name(col: str) -> str:
    """
    Clean column name by:
    - Removing smart quotes and any content within them
    - Removing encoding artifacts (Â, �, etc.)
    - Stripping accents, special chars
    """
    # Remove corrupted or smart quoted text like “Nescafe”
    col = re.sub(r'[“”"‘’\'].*?[“”"‘’\']', '', col)

    # Remove known encoding artifacts
    col = col.replace('Â', '').replace('�', '')

    # Normalize Unicode to ASCII
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('utf-8')

    # Remove any remaining non-ASCII/control characters
    col = re.sub(r'[^\x20-\x7E]', '', col)

    # Normalize whitespace
    col = re.sub(r'\s+', ' ', col)

    return col.strip()

def deduplicate_columns(df: pd.DataFrame, output_log_path: str = "output/column_deduplication_log.csv") -> pd.DataFrame:
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

# Example processing pipeline
def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_empty_columns(df)
    df = clean_column_names(df)  # <-- Ensures final column names are fully cleaned
    df = deduplicate_columns(df)
    return df
