import pandas as pd
from etl.cleaner import clean_dataframe
from etl.dims import (
    entities, assessment, crops, fields,
    soil, climate, waste, geolocation
)
from etl.facts import (
    results, emissions, audit, land_use
)

def load_data(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path)
    df = clean_dataframe(df)
    return df

def run_etl_pipeline(df: pd.DataFrame) -> dict:
    return {
        # Dimensions
        'dim_entities': entities.build_dim_entities(df),
        'dim_assessments': assessment.build_dim_assessment(df),
        'dim_crops': crops.build_dim_crops(df),
        'dim_fields': fields.build_dim_fields(df),
        'dim_soil': soil.build_dim_soil(df),
        'dim_climate': climate.build_dim_climate(df),
        'dim_waste': waste.build_dim_waste(df),
        'dim_geolocation': geolocation.build_dim_geolocation(df),

        # Facts
        'fact_results': results.build_fact_results(df),
        'fact_emissions': emissions.build_fact_emissions(df),
        'fact_audit': audit.build_fact_audit(df),
        'fact_land_use': land_use.build_fact_land_use(df),
    }
