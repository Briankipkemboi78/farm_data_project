import os
import pandas as pd
from models.models import (
    build_dim_entities, build_dim_education, build_dim_identification,
    build_dim_species, build_dim_farm_detail, build_dim_country,
    build_dim_region, build_dim_subregion, build_dim_contact_details, build_dim_plot_details,
    build_fact_irrigation_and_water,build_fact_soil, build_first_cash_crop, build_fact_training_data,
    build_fact_survey_data
)
from models.utils import deduplicate_columns, drop_empty_columns, clean_entity_id, process_dataframe

# Create output directory
os.makedirs("output", exist_ok=True)

# Load raw data
df_raw = pd.read_csv("raw_data/raw_data.csv", low_memory=False, encoding="ISO-8859-1")
df_cft = pd.read_excel("raw_data/cft.xlsx")
df_cft = process_dataframe(df_cft)

# Deduplicate duplicated/variant columns (e.g., .1, (Update)) # for 
df = deduplicate_columns(df_raw, output_log_path="output/column_deduplication_log.csv")

# 2. Drop null-like columns (entirely NaN / blank / dash)
df = drop_empty_columns(df, log_path="output/null_columns_dropped.csv")

# 3. Ensure column names are unique after cleaning
df = df.loc[:, ~df.columns.duplicated()]

# Save cleaned dataset for traceability
df.to_csv("output/cleaned_data_for_modeling.csv", index=False)

# ----------------------
# Build Dimension Tables from 
# ----------------------
dim_entities = build_dim_entities(df)
dim_education = build_dim_education(df)
dim_identification = build_dim_identification(df)
dim_species = build_dim_species(df)
dim_farm_detail = build_dim_farm_detail(df)
dim_country = build_dim_country(df)
dim_region = build_dim_region(df, dim_country)
dim_subregion = build_dim_subregion(df, dim_region)
dim_contact = build_dim_contact_details(df, dim_subregion)
dim_plot_detail, df = build_dim_plot_details(df)

dim_first_cash_crop = build_first_cash_crop(df)
#dim_second_cash_crop = build_second_cash_crop(df)

# -----------------------------------
# Merge foreign keys for fact table
# -----------------------------------
df = df.merge(dim_education, on='Education Level', how='left')
df = df.merge(dim_species, on='Species', how='left')
df = df.rename(columns={
    'Entity ID': 'entity_id',
    'Address': 'address',
    'Phone Number': 'phone_number',
    'Email': 'email',
    'Sub Region': 'sub_region'
})
df = df.merge(dim_subregion, on='sub_region', how='left')
df = df.merge(dim_contact, on=['entity_id', 'address', 'phone_number', 'email', 'subregion_id'], how='left')

# ---------------------
# Build Fact Table
# ---------------------
fact_irrigation_water = build_fact_irrigation_and_water(df)
fact_soil = build_fact_soil(df)
fact_training = build_fact_training_data(df)
fact_survey = build_fact_survey_data(df)

# ---------------------
# Save Output Tables
# ---------------------
dim_entities.to_csv("output/dim_entities.csv", index=False)
dim_education.to_csv("output/dim_education.csv", index=False)
dim_identification.to_csv("output/dim_identification.csv", index=False)
dim_species.to_csv("output/dim_species.csv", index=False)
dim_farm_detail.to_csv("output/dim_farm_detail.csv", index=False)
dim_country.to_csv("output/dim_country.csv", index=False)
dim_region.to_csv("output/dim_region.csv", index=False)
dim_subregion.to_csv("output/dim_subregion.csv", index=False)
dim_contact.to_csv("output/dim_contact_details.csv", index=False)
dim_plot_detail.to_csv("output/dim_plot_detail.csv", index=False)
fact_irrigation_water.to_csv("output/fact_irrigation_and_water.csv", index=False)
fact_soil.to_csv("output/fact_soil.csv", index=False)
fact_training.to_csv("output/fact_training.csv", index=False)
dim_first_cash_crop.to_csv("output/dim_first_cash_crop.csv", index=False)
#dim_second_cash_crop.to_csv("output/dim_second_cash_crop.csv", index=False)
fact_survey.to_csv("output/fact_survey.csv", index=False)

# ---------------------
# Validation & Summary
# ---------------------
if dim_entities['entity_id'].is_unique:
    print("✅ entity_id is unique")
else:
    print("❌ Duplicate entity_id found!")

print("✔️ All dimension and fact tables generated and saved.")

# Save list of final fact columns
fact_columns = pd.DataFrame(fact_survey.columns, columns=["column_name"])
fact_columns.to_csv("output/fact_survey_columns.csv", index=False)
print("📝 fact_survey column names saved to output/fact_survey_columns.csv")

#CFT Columns
column_list = pd.DataFrame(df_cft.columns, columns=["column_name"])
column_list.to_csv("output/cft_column_list.csv", index=False)

