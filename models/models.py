import pandas as pd
import numpy as np
import logging
from models.utils import clean_entity_id

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Context object to store used columns
class Context:
    used_columns = set()

# Dimension: Entities
def build_dim_entities(df: pd.DataFrame) -> pd.DataFrame:
    used_cols = ['Entity ID', 'Entity Name', 'First Name', 'Last Name', 'Gender', 'Year of Birth']
    Context.used_columns.update(used_cols)
    df = clean_entity_id(df)
    entities_df = df[used_cols].copy()
    entities_df.columns = ['entity_id', 'entity_name', 'first_name', 'last_name', 'gender', 'year_of_birth']
    return entities_df.drop_duplicates().reset_index(drop=True)

# Dimension: Education
def build_dim_education(df: pd.DataFrame) -> pd.DataFrame:
    used_cols = ['Education Level (Update)']
    Context.used_columns.update(used_cols)
    dim_education = df[used_cols].drop_duplicates().dropna().reset_index(drop=True)
    dim_education['education_id'] = dim_education.index + 1
    return dim_education

# Dimension: Identification
def build_dim_identification(df: pd.DataFrame) -> pd.DataFrame:
    used_cols = ['Entity ID', 'Identification ID (Update)', 'Identification ID Type (Update)']
    Context.used_columns.update(used_cols)
    identification_df = df[used_cols].copy()
    identification_df.columns = ['entity_id', 'identification_id', 'identification_id_type']
    return identification_df.drop_duplicates().reset_index(drop=True)

# Dimension: Species
def build_dim_species(df: pd.DataFrame) -> pd.DataFrame:
    used_cols = ['Species']
    Context.used_columns.update(used_cols)
    dim_species = df[used_cols].drop_duplicates().dropna().reset_index(drop=True)
    dim_species['species_id'] = dim_species.index + 1
    return dim_species

# Dimension: Farm Detail
def build_dim_farm_detail(df: pd.DataFrame) -> pd.DataFrame:
    used_cols = ['Entity ID', 'Latitude (update)', 'Longitude (update)', 'Altitude (update)',
                 'Polygon (update)', 'Calculate Polygon Area (update)',
                 'Total Farm Area (Update)', 'Existing Plot - Plot number']
    Context.used_columns.update(used_cols)
    farm_detail_df = df[used_cols].copy()
    farm_detail_df.columns = ['entity_id', 'latitude', 'longitude', 'altitude', 'polygon', 'polygon_area',
                              'total_farm_area', 'plot_number']
    return farm_detail_df.drop_duplicates().reset_index(drop=True)

# Dimension: Plot Detail
def build_dim_plot_details(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rename_map = {
        'Entity ID': 'entity_id',
        'New Plot - Plot number': 'plot_number'
    }
    df = df.rename(columns=rename_map)
    Context.used_columns.update(rename_map.keys())

    used_cols = ['entity_id', 'plot_number', 'Year Started', 'Production',
                 'Total Coffee Area', 'Coffee Area Under Active Production',
                 'Coffee Area Under Rejuvenation Stage (less than 3 years)']
    Context.used_columns.update(used_cols)

    plot_df = df[used_cols].copy()
    plot_df.columns = ['entity_id', 'plot_number', 'year_started', 'production',
                       'total_coffee_area', 'active_coffee_area', 'rejuvenation_area']

    plot_df = plot_df.drop_duplicates().reset_index(drop=True)
    plot_df['plot_id'] = plot_df.groupby('entity_id').cumcount() + 1
    plot_df['plot_id'] = plot_df.apply(lambda row: f"{row['entity_id']}_{row['plot_id']}", axis=1)
    plot_df = plot_df[['plot_id'] + [col for col in plot_df.columns if col != 'plot_id']]
    df = df.merge(plot_df[['entity_id', 'plot_number', 'plot_id']], on=['entity_id', 'plot_number'], how='left')
    return plot_df, df

# Dimension: Country
def build_dim_country(df: pd.DataFrame) -> pd.DataFrame:
    used_cols = ['Country']
    Context.used_columns.update(used_cols)
    dim_country = df[used_cols].drop_duplicates().dropna().reset_index(drop=True)
    dim_country['country_id'] = dim_country.index + 1
    return dim_country

# Dimension: Region
def build_dim_region(df: pd.DataFrame, dim_country: pd.DataFrame) -> pd.DataFrame:
    rename_map = {'Region (update)': 'region'}
    df = df.merge(dim_country, on='Country', how='left').rename(columns=rename_map)
    Context.used_columns.update(rename_map.keys())
    dim_region = df[['region', 'country_id']].drop_duplicates().dropna().reset_index(drop=True)
    dim_region['region_id'] = dim_region.index + 1
    return dim_region

# Dimension: Subregion
def build_dim_subregion(df: pd.DataFrame, dim_region: pd.DataFrame) -> pd.DataFrame:
    rename_map = {'Region (update)': 'region', 'Sub Region (update)': 'sub_region'}
    df = df.rename(columns=rename_map)
    Context.used_columns.update(rename_map.keys())
    df = df.merge(dim_region, on='region', how='left')
    dim_subregion = df[['sub_region', 'region_id']].drop_duplicates().dropna().reset_index(drop=True)
    dim_subregion['subregion_id'] = dim_subregion.index + 1
    return dim_subregion

# Dimension: Contact Details
def build_dim_contact_details(df: pd.DataFrame, dim_subregion: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        'Entity ID': 'entity_id',
        'Sub Region (update)': 'sub_region',
        'Address (Update)': 'address',
        'Phone Number (Update)': 'phone_number',
        'Email (Update)': 'email'
    }
    df = df.rename(columns=rename_map)
    Context.used_columns.update(rename_map.keys())
    df = df.merge(dim_subregion, on='sub_region', how='left')
    dim_contact = df[['entity_id', 'address', 'phone_number', 'email', 'subregion_id']].drop_duplicates().reset_index(drop=True)
    dim_contact['contact_id'] = dim_contact.index + 1
    return dim_contact

# Fact Table
def build_fact_survey_data(df: pd.DataFrame) -> pd.DataFrame:
    dropped_cols = [col for col in Context.used_columns if col in df.columns]
    logging.info(f"🧹 Dropping used columns from fact_survey: {dropped_cols}")
    df_cleaned = df.drop(columns=dropped_cols)
    return df_cleaned.dropna(axis=1, how='all')
