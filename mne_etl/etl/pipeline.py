import pandas as pd
from etl.utils.utils import process_dataframe
from etl.utils.secondary_cleaner import clean_dataframe as clean_cft

# Dimension Models
from etl.dim.dim_entity import build_dim_entity
from etl.dim.dim_location import build_dim_location
from etl.dim.dim_farmer import build_dim_farmer
from etl.dim.dim_plot import build_dim_plot
from etl.dim.dim_identification import build_dim_identification
from etl.dim.dim_education import build_dim_education

# Fact Models
from etl.fact.fact_soil_assessment import build_fact_soil_assessment
from etl.fact.fact_biodiversity_assessment import build_fact_biodiversity_assessment
from etl.fact.fact_water_management import build_fact_water_management
from etl.fact.fact_agro_inputs import build_fact_agro_inputs
from etl.fact.fact_economics import build_fact_economics
from etl.fact.fact_recordkeeping import build_fact_recordkeeping
from etl.fact.fact_nescafe_plan import build_fact_nescafe_plan
from etl.fact.fact_revenue_economics import build_fact_revenue_economics

# Feedback Models
from etl.feedback.fact_feedback_demographics import build_fact_feedback_demographics
from etl.feedback.fact_feedback_agronomy import build_fact_feedback_agronomy
from etl.feedback.fact_feedback_climate import build_fact_feedback_climate
from etl.feedback.fact_feedback_programs import build_fact_feedback_programs
from etl.feedback.fact_feedback_validator import build_fact_feedback_validator
from etl.feedback.fact_survey_feedback import build_fact_survey_feedback

# CFT Models
from etl.dim.dim_entity_cft import build_dim_entity_cft
from etl.dim.dim_geolocation_cft import build_dim_geolocation


from etl.fact.fact_energy_use_cft import build_fact_energy_usage
from etl.fact.fact_co_product import build_fact_co_product

def run_pipeline(df_raw: pd.DataFrame, 
                 df_cft: pd.DataFrame,
                 df_co_product_cleaned: pd.DataFrame,
                 df_energy: pd.DataFrame,
                 df_fertilizer_input: pd.DataFrame) -> dict:


    df_clean = process_dataframe(df_raw)
    df_cft_clean = clean_cft(df_cft)

    # 🧱 Core Output Dictionary
    results = {}
    used_columns = set()

    # ✨ Dimension Models
    dim_functions = {
        'dim_entity': build_dim_entity,
        'dim_location': build_dim_location,
        'dim_farmer': build_dim_farmer,
        'dim_plot': build_dim_plot,
        'dim_identification': build_dim_identification,
        'dim_education': build_dim_education
    }

    for name, func in dim_functions.items():
        results[name] = func(df_clean)

    dim_education_df = results['dim_education']
    used_columns.update(col for df in results.values() for col in df.columns)

    # 📊 Fact Models
    fact_functions = {
        'fact_soil_assessment': build_fact_soil_assessment,
        'fact_biodiversity_assessment': build_fact_biodiversity_assessment,
        'fact_water_management': build_fact_water_management,
        'fact_agro_inputs': build_fact_agro_inputs,
        'fact_economics': build_fact_economics,
        'fact_recordkeeping': build_fact_recordkeeping,
        'fact_nescafe_plan': build_fact_nescafe_plan,
        'fact_revenue_economics': build_fact_revenue_economics
    }

    for name, func in fact_functions.items():
        results[name] = func(df_clean)

    # 💬 Feedback Models
    results['fact_feedback_demographics'] = build_fact_feedback_demographics(df_clean, dim_education_df)

    feedback_funcs = {
        'fact_feedback_agronomy': build_fact_feedback_agronomy,
        'fact_feedback_climate': build_fact_feedback_climate,
        'fact_feedback_programs': build_fact_feedback_programs,
        'fact_feedback_validator': build_fact_feedback_validator
    }

    for name, func in feedback_funcs.items():
        results[name] = func(df_clean)

    used_columns.update(
        results['fact_feedback_agronomy'].question_label.tolist() +
        results['fact_feedback_climate'].question_label.tolist() +
        results['fact_feedback_programs'].question_label.tolist() +
        results['fact_feedback_validator'].question_label.tolist()
    )

    results['fact_survey_feedback'] = build_fact_survey_feedback(df_clean, dim_education_df, used_columns)

    # 🌱 CFT Dimensions and Lookup
    dim_entity_cft_df = build_dim_entity_cft(df_cft_clean)
    results['dim_entity_cft'] = dim_entity_cft_df

    entity_lookup = dict(zip(dim_entity_cft_df['entity_id'], dim_entity_cft_df['entity_id']))
    results['dim_geolocation_cft'] = build_dim_geolocation(df_cft_clean)

    # ⚡️ Energy Usage Fact
    results['fact_energy_cft'] = build_fact_energy_usage(df_energy, entity_lookup)
    results['fact_co_product_cft'] = build_fact_co_product(df_co_product_cleaned, entity_lookup)

    return results
