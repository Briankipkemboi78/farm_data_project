import pandas as pd

# Load and clean base data
df = pd.read_csv("raw_data/raw_data.csv", low_memory=False, encoding="ISO-8859-1")
df = df.loc[:, ~df.columns.duplicated()]

# Step 1: Create dim_units
dim_units = df[['Unit']].drop_duplicates().dropna().reset_index(drop=True)
dim_units['unit_id'] = dim_units.index + 1
dim_units = dim_units[['unit_id', 'Unit']]
dim_units.columns = ['unit_id', 'unit_name']

# Step 2: Create dim_countries
dim_countries = df[['Country']].drop_duplicates().dropna().reset_index(drop=True)
dim_countries['country_id'] = dim_countries.index + 1
dim_countries = dim_countries[['country_id', 'Country']]
dim_countries.columns = ['country_id', 'country_name']

# Step 3: Create dim_entities
entities_df = df[['Entity ID', 'System ID', 'Entity Name', 'Unit', 'Country']].copy()
entities_df.columns = ['entity_id', 'system_id', 'entity_name', 'unit_name', 'country_name']

entities_df['entity_id'] = entities_df['entity_id'].astype(str).str.lstrip("'")

# Join with dim_units and dim_countries to get IDs
entities_df = entities_df.merge(dim_units, on='unit_name', how='left')
entities_df = entities_df.merge(dim_countries, on='country_name', how='left')

# Final normalized entity table
dim_entities = entities_df[['entity_id', 'system_id', 'entity_name', 'unit_id', 'country_id']].drop_duplicates()

# Step 4: Save all to seeds folder
dim_units.to_csv("seeds/dim_units.csv", index=False)
dim_countries.to_csv("seeds/dim_countries.csv", index=False)
dim_entities.to_csv("seeds/dim_entities.csv", index=False)
