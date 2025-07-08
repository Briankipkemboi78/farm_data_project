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

    # Select required columns
    education_level_df = df[
        ['Entity ID', 'Education Level (Update)']
    ].copy()

    # Rename columns for consistency
    education_level_df.columns = [
        'entity_id',  'education_level'
    ]

    # Drop duplicates to ensure uniqueness
    education_level_df = education_level_df.drop_duplicates().reset_index(drop=True)

    return education_level_df


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
