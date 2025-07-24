import pandas as pd
import os

from etl.utils.utils import process_dataframe
from etl.utils.secondary_cleaner import clean_dataframe as clean_cft
from etl.pipeline import run_pipeline

def main():
    # Resolve paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    raw_dir = os.path.join(script_dir, "data", "raw")
    output_dir = os.path.join(script_dir, "data", "cleaned")
    os.makedirs(output_dir, exist_ok=True)

    # Load and clean main CSV dataset
    df_raw = pd.read_csv(os.path.join(raw_dir, "raw_data.csv"), encoding='ISO-8859-1', low_memory=False)
    df_cleaned = process_dataframe(df_raw)

    # Save cleaned main data (optional inspection)
    cleaned_file_path = os.path.join(output_dir, "cleaned_raw_input.csv")
    df_cleaned.to_csv(cleaned_file_path, index=False)

    # Load and clean CFT Excel (first sheet only)
    df_cft_raw = pd.read_excel(os.path.join(raw_dir, "cft.xlsx"), sheet_name=0)
    df_cft_cleaned = clean_cft(df_cft_raw)

    df_co_product = pd.read_excel(os.path.join(raw_dir, "cft.xlsx"), sheet_name=1)
    df_co_product_cleaned = clean_cft(df_co_product)

    df_energy = pd.read_excel(os.path.join(raw_dir, "cft.xlsx"), sheet_name=4)
    df_energy = clean_cft(df_energy)

    df_fertilizer_input = pd.read_excel(os.path.join(raw_dir, "cft.xlsx"), sheet_name=1)
    df_fertilizer_input = clean_cft(df_fertilizer_input)

    # Run pipeline on the cleaned datasets
    results = run_pipeline(df_cleaned, df_cft_cleaned, df_co_product_cleaned, df_energy, df_fertilizer_input)

    # Saving outputs
    for name, df in results.items():
        output_path = os.path.join(output_dir, f"{name}.csv")
        df.to_csv(output_path, index=False)

    print(f"✅ ETL pipeline complete.\n📁 Cleaned input: {cleaned_file_path}\n📁 Outputs saved to: {output_dir}")

if __name__ == "__main__":
    main()
