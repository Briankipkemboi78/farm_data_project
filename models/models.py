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
    used_cols = ['Education Level']
    Context.used_columns.update(used_cols)
    dim_education = df[used_cols].drop_duplicates().dropna().reset_index(drop=True)
    dim_education['education_id'] = dim_education.index + 1
    return dim_education

# Dimension: Identification
def build_dim_identification(df: pd.DataFrame) -> pd.DataFrame:
    used_cols = ['Entity ID', 'Identification ID', 'Identification ID Type']
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
    used_cols = [
        'Entity ID', 'Latitude', 'Longitude', 'Altitude', 'Calculate Polygon Area',
        'Total Farm Area', 'Existing Plot - Plot number'
    ]

    # Safety check for missing columns
    missing_cols = [col for col in used_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing expected farm detail columns: {missing_cols}")

    Context.used_columns.update(used_cols)

    farm_detail_df = df[used_cols].copy()
    farm_detail_df.columns = [
        'entity_id', 'latitude', 'longitude', 'altitude', 'polygon_area', 'total_farm_area', 'plot_number'
    ]

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
   
    used_cols = ['Country', 'Region']

    missing_cols = [col for col in used_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing expected region columns: {missing_cols}")
    Context.used_columns.update(used_cols)

    df = df.merge(dim_country, on='Country', how='left')

    dim_region = df[['Region', 'country_id']].drop_duplicates().dropna().reset_index(drop=True)
    dim_region.columns = ['region', 'country_id']
    dim_region['region_id'] = dim_region.index + 1

    return dim_region

# Dimension: Subregion
def build_dim_subregion(df: pd.DataFrame, dim_region: pd.DataFrame) -> pd.DataFrame:
    expected_cols = ['Region', 'Sub Region']

    # Safety check for required columns
    missing_cols = [col for col in expected_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing expected subregion columns: {missing_cols}")

    # Rename for consistency
    rename_map = {
        'Region': 'region',
        'Sub Region': 'sub_region'
    }
    df = df.rename(columns=rename_map)
    Context.used_columns.update(rename_map.keys())

    # Merge with region dimension to get region_id
    df = df.merge(dim_region, on='region', how='left')

    # Build subregion dimension
    dim_subregion = df[['sub_region', 'region_id']].drop_duplicates().dropna().reset_index(drop=True)
    dim_subregion['subregion_id'] = dim_subregion.index + 1

    return dim_subregion

# Dimension: Contact Details
def build_dim_contact_details(df: pd.DataFrame, dim_subregion: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        'Entity ID': 'entity_id',
        'Sub Region': 'sub_region',
        'Address': 'address',
        'Phone Number': 'phone_number',
        'Email': 'email'
    }
    df = df.rename(columns=rename_map)
    Context.used_columns.update(rename_map.keys())
    df = df.merge(dim_subregion, on='sub_region', how='left')
    dim_contact = df[['entity_id', 'address', 'phone_number', 'email', 'subregion_id']].drop_duplicates().reset_index(drop=True)
    dim_contact['contact_id'] = dim_contact.index + 1
    return dim_contact


# Dimension: Household Demographics
def build_dim_household(df: pd.DataFrame) -> pd.DataFrame:
    # Define column groups based on name patterns
    column_groups = {
        'no_of_adults': [col for col in df.columns if 'Number of adults in farmers family' in col],
        'no_of_boys': [col for col in df.columns if 'Number of boys' in col],
        'no_of_girls': [col for col in df.columns if 'Number of girls' in col]
    }

    # Helper: Select column with the most non-null values
    def select_most_complete_column(cols):
        return df[cols].loc[:, df[cols].notna().sum().idxmax()]

    # Build normalized DataFrame
    household_df = pd.DataFrame()
    for new_col, col_list in column_groups.items():
        household_df[new_col] = select_most_complete_column(col_list)
        Context.used_columns.update(col_list)  # Track all variants as used

    # Add entity_id
    if 'Entity ID' in df.columns:
        household_df['entity_id'] = df['Entity ID']
        Context.used_columns.add('Entity ID')
    else:
        household_df['entity_id'] = ['HH' + str(i + 1) for i in range(len(household_df))]

    # Reorder
    household_df = household_df[['entity_id', 'no_of_adults', 'no_of_boys', 'no_of_girls']]
    return household_df.drop_duplicates().reset_index(drop=True)


# water
def build_fact_irrigation_and_water(df: pd.DataFrame) -> pd.DataFrame:
    entity_col = 'Entity ID' if 'Entity ID' in df.columns else 'entity_id'
    used_cols = [
        entity_col,
        'Year of reporting',
        'Do you irrigate your coffee?',
        'Do you know the water usage?',
        'Do you conduct wet processing at the farm?',
        'Do you consistent monitor and record total water usage for irrigation at the farm  - if applicable',
        'Do you consistent monitor and record total water usage for wet processing at the farm - if applicable',
        'Water use in Irrigation (m3 water / ha / year)',
        'Water use in wet processing (m3 water / kg parchment coffee)',
        'Is the Waste water treatment adequate vs production volume',
        'Do you have water bodies at your farm?',
        'If you have water bodies at your farm, what is the minimum distance between field (fertilizer and pesticide application area) and the water body?',
        'Are the riparian buffer strips covered with natural vegetation (hedges, bushes, trees, etc)?',
        'Where does the irrigation water come from?',
        'How many irrigation rounds/year?',
        'How much irrigation water per round? (m3/ha/round)'
    ]

    # Optional safety check
    missing_cols = [col for col in used_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing columns in irrigation and water model: {missing_cols}")

    # Track used columns to drop from fact later
    Context.used_columns.update(used_cols)

    irrigation_df = df[used_cols].copy()

    irrigation_df.columns = [
        'entity_id',
        'year_of_reporting',
        'irrigates_coffee',
        'knows_water_usage',
        'wet_processing_at_farm',
        'monitors_irrigation_usage',
        'monitors_wet_processing_usage',
        'water_use_irrigation_m3_per_ha',
        'water_use_wet_processing_m3_per_kg',
        'adequate_wastewater_treatment',
        'has_water_bodies',
        'distance_to_water_body',
        'riparian_buffer_vegetation',
        'irrigation_source',
        'irrigation_rounds_per_year',
        'irrigation_water_per_round_m3_per_ha'
    ]

    cols_to_check = irrigation_df.columns.difference(['entity_id', 'year_of_reporting'])
    irrigation_df = irrigation_df.dropna(subset=cols_to_check, how='all')

    return irrigation_df.drop_duplicates().reset_index(drop=True)

#Soil
def build_fact_soil(df: pd.DataFrame) -> pd.DataFrame:
    entity_col = 'Entity ID' if 'Entity ID' in df.columns else 'entity_id'

    used_cols = [
        entity_col,
        'Year of reporting',  # <-- now using existing column
        'What is the percentage of coffee crop land covered, during the whole year, with cover crops and/or application of crop residues, mulch, grass, clipping, straw and/or through agroforestry, coffee canopy, etc.? (acreage covered/total coffee acreage x 100)',
        'Do you implement any form of erosion control (e.g. terracing, contour planting, windbreaks, soil coverage, basin - on 100% of the field acreage)?',
        'What is the percentage of agricultural land with severe water and wind erosion (signs: siltation, sheet-rill-gully erosion, flying dust; as estimation)?',
        'Do you perform regularly a soil analysis (lab, soil test kit)?',
        'On average, what is the interval of soil analysis (lab, soil test kit) for texture, pH, SOM, nitrogen (N), phosphorus (P), and potassium (K) (farm sample or a representative sample of a group of smallholdings in the same area)',
        'Do you calculate your annual fertilizer plan on the basis of crop nutrient requirements (e.g. recent soil analysis, productivity, crop cycle)?',
        'What is the soil organic matter (SOM) level in your soils (%)?',
        'What is the Soil pH?',
        'What is the percentage of coffee crop land that receives, annually, organic fertilizer, and/or composted organic matter, and/or biochar?',
        'Organic fertilizer applied kg per ha',
        'Total fertilizer applied kg per ha',
        'Percentage organic fertiliser vs total fertiliser applied per ha (organic/ total applied x 100)',
        'What is the most frequently used fertilizer',
        'N%',
        'P%',
        'K%',
        'How much you apply per year',
        'How many ha in the coffee area',
        'Yield GC per ha',
        'N Productivity (NP) = Y/N (Yield kg GC per ha / N kg applied per ha)',
        'Are you applying 4C unacceptable and/or red listed agro-inputs (insecticides, fungicides, herbicides) in your farm?',
        'How often are you applying herbicides (per year)',
        'Which integrated weed management practices do you apply (on 100% of the field acreage)?'
    ]

    missing_cols = [col for col in used_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing columns in soil fact model: {missing_cols}")

    Context.used_columns.update(used_cols)

    soil_df = df[used_cols].copy()

    soil_df.columns = [
        'entity_id',
        'year_of_reporting',
        'coffee_land_with_cover_crop_percent',
        'has_erosion_control',
        'agricultural_land_with_severe_erosion_percent',
        'performs_soil_analysis',
        'soil_analysis_interval',
        'calculates_fertilizer_plan',
        'soil_organic_matter_percent',
        'soil_ph',
        'coffee_land_receiving_organic_fertilizer_percent',
        'organic_fertilizer_kg_per_ha',
        'total_fertilizer_kg_per_ha',
        'organic_fertilizer_percent_of_total',
        'most_used_fertilizer',
        'n_percent',
        'p_percent',
        'k_percent',
        'fertilizer_applied_per_year',
        'coffee_area_ha',
        'yield_gc_per_ha',
        'n_productivity_np',
        'uses_red_listed_agro_inputs',
        'herbicide_application_frequency_per_year',
        'weed_management_practices'
    ]

    soil_df = soil_df.dropna(
        subset=soil_df.columns.difference(['entity_id', 'year_of_reporting']),
        how='all'
    )

    return soil_df.drop_duplicates().reset_index(drop=True)





# cash_crop_1

def build_first_cash_crop(df: pd.DataFrame) -> pd.DataFrame:
    entity_col = 'Entity ID' if 'Entity ID' in df.columns else 'entity_id'
    
    used_cols = [
        entity_col,
        '1st main cash crop beside coffee'
    ]

    # Check for missing columns
    missing_cols = [col for col in used_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing columns in First Cash Crop model: {missing_cols}")

    # Track used columns
    Context.used_columns.update(used_cols)

    crop_df = df[used_cols].copy()

    # Rename columns to standard names for fact table
    crop_df.columns = [
        'entity_id',
        'main_cash_crop_name'
    ]

    # Drop rows with no data except entity_id
    crop_df = crop_df.dropna(
        subset=crop_df.columns.difference(['entity_id']),
        how='all'
    )

    return crop_df.drop_duplicates().reset_index(drop=True)


# Fact Table
def build_fact_survey_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the fact_survey table by:
    - Dropping columns already used in dimension tables
    - Dropping suffix variants (.1, .2, .3) just in case
    - Dropping any columns with all nulls
    """
    # Drop columns used in dimension tables
    dropped_cols = [col for col in Context.used_columns if col in df.columns]

    # Drop columns that still have unwanted suffixes
    suffixes = ['.1', '.2', '.3']
    suffix_cols_to_drop = [col for col in df.columns if any(col.endswith(suffix) for suffix in suffixes)]

    # Combine and ensure uniqueness
    all_cols_to_drop = list(set(dropped_cols + suffix_cols_to_drop))

    # Drop the columns
    df_cleaned = df.drop(columns=all_cols_to_drop, errors='ignore')

    # Drop columns that are entirely NaN
    df_cleaned = df_cleaned.dropna(axis=1, how='all')

    return df_cleaned



