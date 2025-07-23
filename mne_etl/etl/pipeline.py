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

# CFT

from etl.dim.dim_entity_cft import build_dim_entity_cft
#from etl.dim.dim_field_geolocation_cft import build_dim_field_geolocation_cft
#from etl.fact.fact_assessment_cft import build_fact_assessment_cft
# from etl.fact.fact_climate_crops_cft import build_fact_climate_crops_cft
# from etl.fact.fact_soil_cft import build_fact_soil_cft
# from etl.fact.fact_waste_management_cft import build_fact_waste_management_cft

def run_pipeline(df_raw: pd.DataFrame, df_cft: pd.DataFrame) -> dict:
    df_clean = process_dataframe(df_raw)
    df_cft_clean = clean_cft(df_cft)
    results = {}
    used_columns = set()

    # Dimensions
    results['dim_entity'] = build_dim_entity(df_clean)
    results['dim_location'] = build_dim_location(df_clean)
    results['dim_farmer'] = build_dim_farmer(df_clean)
    results['dim_plot'] = build_dim_plot(df_clean)
    results['dim_identification'] = build_dim_identification(df_clean)
    results['dim_education'] = build_dim_education(df_clean)

    for df_name in results:
        used_columns.update(results[df_name].columns)

    # Facts
    results['fact_soil_assessment'] = build_fact_soil_assessment(df_clean)
    results['fact_biodiversity_assessment'] = build_fact_biodiversity_assessment(df_clean)
    results['fact_water_management'] = build_fact_water_management(df_clean)
    results['fact_agro_inputs'] = build_fact_agro_inputs(df_clean)
    results['fact_economics'] = build_fact_economics(df_clean)
    results['fact_recordkeeping'] = build_fact_recordkeeping(df_clean)
    results['fact_nescafe_plan'] = build_fact_nescafe_plan(df_clean)

    results['fact_revenue_economics'] = build_fact_revenue_economics(df_clean)

    # Feedback
    results['fact_feedback_demographics'] = build_fact_feedback_demographics(df_clean)
    results['fact_feedback_agronomy'] = build_fact_feedback_agronomy(df_clean)
    results['fact_feedback_climate'] = build_fact_feedback_climate(df_clean)
    results['fact_feedback_programs'] = build_fact_feedback_programs(df_clean)
    results['fact_feedback_validator'] = build_fact_feedback_validator(df_clean)

    used_columns.update(
        results['fact_feedback_demographics'].question_label.tolist() +
        results['fact_feedback_agronomy'].question_label.tolist() +
        results['fact_feedback_climate'].question_label.tolist() +
        results['fact_feedback_programs'].question_label.tolist() +
        results['fact_feedback_validator'].question_label.tolist()
    )

    results['fact_survey_feedback'] = build_fact_survey_feedback(df_clean, used_columns)

    #CFT 
    # CFT models for df_cft_clean
    results['dim_entity_cft'] = build_dim_entity_cft(df_cft_clean)
    #results['dim_field_geolocation_cft'] = build_dim_field_geolocation_cft(df_cft_clean)
    # results['fact_assessment_cft'] = build_fact_assessment_cft(df_cft_clean)
    # results['fact_climate_crops_cft'] = build_fact_climate_crops_cft(df_cft_clean)
    # results['fact_soil_cft'] = build_fact_soil_cft(df_cft_clean)
    # results['fact_waste_management_cft'] = build_fact_waste_management_cft(df_cft_clean)


    return results
