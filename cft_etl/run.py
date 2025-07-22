import os
from main import load_data, run_etl_pipeline

INPUT_PATH = r'C:\Users\BrianKipkemboi\Desktop\Brian\farm_data_project\cft_etl\data\cft.xlsx'
OUTPUT_DIR = 'output'

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

df = load_data(INPUT_PATH)
tables = run_etl_pipeline(df)

for name, table in tables.items():
    table.to_csv(f"{OUTPUT_DIR}/{name}.csv", index=False)
    print(f"{name.upper()} exported ✅ — {len(table)} rows")
