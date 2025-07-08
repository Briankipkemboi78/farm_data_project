import os
import pandas as pd
from models.models import (
    build_dim_entities,
    build_dim_education,
    build_dim_identification
)

# Ensure the output directory exists
os.makedirs("output", exist_ok=True)

# Step 1: Load base raw data
df = pd.read_csv("raw_data/raw_data.csv", low_memory=False, encoding="ISO-8859-1")
df = df.loc[:, ~df.columns.duplicated()]  # Drop duplicate columns if any

# Step 2: Build normalized dimension tables
dim_entities = build_dim_entities(df)
dim_education = build_dim_education(df)
dim_identification = build_dim_identification(df)

# Step 3: Entity ID validation
if dim_entities['entity_id'].is_unique:
    print("✅ All entity_id values are unique.")
else:
    duplicates = dim_entities[dim_entities.duplicated('entity_id', keep=False)]
    print("❌ Duplicate entity_id values found:")
    print(duplicates)

print(f"📄 Total records in dim_entities: {len(dim_entities)}")
print(f"🔢 Number of unique entity_id values: {dim_entities['entity_id'].nunique()}")

# Step 4: Save all tables to the output folder
dim_entities.to_csv("output/dim_entities.csv", index=False)
dim_education.to_csv("output/dim_education.csv", index=False)
dim_identification.to_csv("output/dim_identification.csv", index=False)

print("✅ All models successfully generated and saved to the 'output/' directory.")
