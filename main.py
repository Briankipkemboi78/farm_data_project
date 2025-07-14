import os
import pandas as pd
from models.models import (
    build_dim_entities,
    build_dim_education,
    build_dim_identification,
    build_dim_species,
    build_dim_farm_detail,
    build_dim_country,
    build_dim_region,
    build_dim_subregion,

    build_fact_survey_data
)

# Create output directory
os.makedirs("output", exist_ok=True)

# Load base raw data
df = pd.read_csv("raw_data/raw_data.csv", low_memory=False, encoding="ISO-8859-1")
df = df.loc[:, ~df.columns.duplicated()]

# Introduce keys and create dimension tables
dim_entities = build_dim_entities(df)
dim_education, df = build_dim_education(df)
dim_identification = build_dim_identification(df)
dim_species, df = build_dim_species(df)
dim_farm_detail = build_dim_farm_detail(df)

dim_country = build_dim_country(df)
dim_region = build_dim_region(df, dim_country)
dim_sub_region = build_dim_subregion(df, dim_region)


fact_survey = build_fact_survey_data(df)

# Now df contains all foreign keys and is ready for use in building fact tables

# Validate entity_id
if dim_entities['entity_id'].is_unique:
    print("All entity_id values are unique.")
else:
    duplicates = dim_entities[dim_entities.duplicated('entity_id', keep=False)]
    print("Duplicate entity_id values found:")
    print(duplicates)

print(f"Total records in dim_entities: {len(dim_entities)}")
print(f"Number of unique entity_id values: {dim_entities['entity_id'].nunique()}")

# Save dimension tables only (not the flat file)
dim_entities.to_csv("output/dim_entities.csv", index=False)
dim_education.to_csv("output/dim_education.csv", index=False)
dim_identification.to_csv("output/dim_identification.csv", index=False)
dim_species.to_csv("output/dim_species.csv", index=False)
dim_farm_detail.to_csv("output/dim_farm_detail.csv", index=False)


dim_country.to_csv("output/dim_country.csv", index=False)
dim_region.to_csv("output/dim_region.csv", index=False)
dim_sub_region.to_csv("output/dim_sub_region.csv", index=False)

fact_survey.to_csv("output/fact_survey.csv", index=False)

print("All dimension models successfully generated and saved.")
