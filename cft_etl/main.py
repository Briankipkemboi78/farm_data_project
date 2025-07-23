import pandas as pd
from etl.cleaner import clean_dataframe
from etl.dims import (
    entities, assessment, crops, fields,
    soil, climate, waste, geolocation
)
from etl.facts import (
    results, emissions, audit, land_use,co_product,
    energy_use, fertilizer_input
)

def load_data(file_path: str) -> tuple:
    # Load main sheet (e.g. CFT)
    df_main = pd.read_excel(file_path, sheet_name='CFT All Crop 2.0')
    df_main = clean_dataframe(df_main)

    # Load Crop Production sheet
    df_co_product = pd.read_excel(file_path, sheet_name='Co Product')
    df_co_product = clean_dataframe(df_co_product)

    # Load Energy Usage sheet
    df_energy = pd.read_excel(file_path, sheet_name='Energy Usage')
    df_energy = clean_dataframe(df_energy)

    df_fertilizer_input = pd.read_excel(file_path, sheet_name='Fertilizer Inputs')
    df_fertilizer_input = clean_dataframe(df_fertilizer_input)


    return df_main, df_co_product, df_energy, df_fertilizer_input

def run_etl_pipeline(
        df_main: pd.DataFrame, 
        df_co_product: pd.DataFrame, 
        df_energy: pd.DataFrame,
        df_fertilizer_input: pd.DataFrame) -> dict:
    dim_entities_df = entities.build_dim_entities(df_main)
    entity_lookup = dict(zip(dim_entities_df['entity_system_id'], dim_entities_df['entity_id']))

    return {
        # Dimensions
        'dim_entities': dim_entities_df,
        'dim_assessments': assessment.build_dim_assessment(df_main),
        'dim_crops': crops.build_dim_crops(df_main),
        'dim_fields': fields.build_dim_fields(df_main),
        'dim_soil': soil.build_dim_soil(df_main),
        'dim_climate': climate.build_dim_climate(df_main),
        'dim_waste': waste.build_dim_waste(df_main),
        'dim_geolocation': geolocation.build_dim_geolocation(df_main),

        # Facts (main sheet)
        'fact_results': results.build_fact_results(df_main),
        'fact_emissions': emissions.build_fact_emissions(df_main),
        'fact_audit': audit.build_fact_audit(df_main),
        'fact_land_use': land_use.build_fact_land_use(df_main),

        # Facts (auxiliary sheets)
        'fact_co_product': co_product.build_fact_co_product(df_co_product, entity_lookup),
        'fact_energy_usage': energy_use.build_fact_energy_usage(df_energy, entity_lookup),
        'fact_fertilizer_input': fertilizer_input.build_fact_fertilizer_input(df_fertilizer_input, entity_lookup),
    }

