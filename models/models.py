import pandas as pd
from models.utils import clean_entity_id


def build_dim_entities(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_entity_id(df)

    entities_df = df[
        ['Entity ID', 'Entity Name', 'First Name', 'Last Name', 'Gender',
         'Identification ID', 'Identification ID Type', 'Year of Birth']
    ].copy()

    entities_df.columns = [
        'entity_id', 'entity_name', 'first_name', 'last_name', 'gender',
        'identification_id', 'identification_id_type', 'year_of_birth'
    ]

    return entities_df.drop_duplicates().reset_index(drop=True)

#Entities model
def build_dim_entities(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_entity_id(df)

    # Select required columns
    entities_df = df[
        ['Entity ID', 'Entity Name', 'First Name', 'Last Name', 'Gender', 'Year of Birth', ]
    ].copy()

    # Rename columns for consistency
    entities_df.columns = [
        'entity_id', 'entity_name', 'first_name', 'last_name', 'gender', 'year_of_birth', 
    ]

    # Drop duplicates to ensure uniqueness
    entities_df = entities_df.drop_duplicates().reset_index(drop=True)

    return entities_df

# Education
def build_dim_education(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_entity_id(df)

     # Create the dimension table
    dim_education = df[['Education Level (Update)']].drop_duplicates().dropna().reset_index(drop=True)
    dim_education['education_id'] = dim_education.index + 1

    # Merge education_id into original df
    df = df.merge(dim_education, on='Education Level (Update)', how='left')

    return dim_education, df


# Identification
def build_dim_identification(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_entity_id(df)

    # Select required columns
    identification_df = df[
        ['Entity ID', 'Identification ID (Update)', 'Identification ID Type (Update)']
    ].copy()

    # Rename columns for consistency
    identification_df.columns = [
        'entity_id',  'identification_id', 'identification_id_type'
    ]

    # Drop duplicates to ensure uniqueness
    identification_df = identification_df.drop_duplicates().reset_index(drop=True)

    return identification_df

# Education
def build_dim_species(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_entity_id(df)

     # Create the dimension table
    dim_species = df[['Species']].drop_duplicates().dropna().reset_index(drop=True)
    dim_species['Species_id'] = dim_species.index + 1

    # Merge education_id into original df
    df = df.merge(dim_species, on='Species', how='left')

    return dim_species, df


# Farm details
def build_dim_farm_detail(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_entity_id(df)

    # Select required columns
    farm_detail_df = df[
        ['Entity ID', 'Latitude (update)', 'Longitude (update)', 'Altitude (update)', 'Polygon (update)', 'Calculate Polygon Area (update)', 
        'Total Farm Area (Update)', 'Existing Plot - Plot number' ]
    ].copy()

    # Rename columns for consistency
    farm_detail_df.columns = [
        'entity_id',  'latitude', 'longitude', 'altitude','polygon', 'polygon_area', 'total_farm_area', 'plot_number'
    ]

    # Drop duplicates to ensure uniqueness
    farm_detail_df = farm_detail_df.drop_duplicates().reset_index(drop=True)

    return farm_detail_df

# Country
def build_dim_country(df: pd.DataFrame):
    dim_country = df[['Country']].drop_duplicates().dropna().reset_index(drop=True)
    dim_country['country_id'] = dim_country.index + 1
    return dim_country

# Region
def build_dim_region(df: pd.DataFrame, dim_country: pd.DataFrame):
    df = df.merge(dim_country, on='Country', how='left')
    dim_region = df[['Region', 'country_id']].drop_duplicates().dropna().reset_index(drop=True)
    dim_region['region_id'] = dim_region.index + 1
    return dim_region

# # subregion
# def build_dim_subregion(df: pd.DataFrame, dim_region: pd.DataFrame):
#     df = df.merge(dim_region, on='Region', how='left')
#     dim_subregion = df[['Subregion', 'region_id']].drop_duplicates().dropna().reset_index(drop=True)
#     dim_subregion.columns = ['entity_id', 'address', 'phone', 'email', 'subregion_id']
#     dim_subregion['subregion_id'] = dim_subregion.index + 1
#     return dim_subregion

# #contact_details
# def build_dim_contact_details(df: pd.DataFrame, dim_subregion: pd.DataFrame):
#     df = df.merge(dim_subregion, on='Subregion', how='left')
#     dim_contact = df[['Entity ID', 'Address (Update)', 'Phone Number (Update)', 'Email (Update)', 'subregion_id']].drop_duplicates()
#     # Rename for consistency
#     dim_contact.columns = ['entity_id', 'address', 'phone_number', 'email', 'subregion_id']
#     dim_contact['contact_id'] = dim_contact.index + 1
#     df = df.merge(dim_contact, on=['Entity ID', 'Address', 'Phone', 'subregion_id'], how='left')
#     return dim_contact, df



# Survery_data
def build_fact_survey_data(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = ['Entity Name', 'First Name', 'Last Name', 'Gender', 'Year of Birth',
        'Education Level (Update)',
        'Identification ID (Update)', 'Identification ID Type (Update)',
        'Species',
        'Latitude (update)', 'Longitude (update)', 'Altitude (update)',
        'GPS method (update)', 'Polygon (update)', 'Calculate Polygon Area (update)',
        'Polygon method (update)', 'Total Farm Area (Update)', 'Existing Plot - Plot number']
    
    # Dropping the columns safely
    df_cleaned = df.drop(columns=[col for col in cols_to_drop if col in df.columns])

    # Dropping columns where all values are NaN
    df_cleaned = df_cleaned.dropna(axis=1, how='all')

    return df_cleaned
    

