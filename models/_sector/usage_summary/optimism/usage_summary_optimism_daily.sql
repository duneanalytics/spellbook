 {{
  config(
        schema = 'usage_summary_optimism',
        alias = 'daily',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['blockchain', 'address', 'block_date'],
        partition_by = ['block_month']
  )
}}

{% set base_models_to_union = [
                                'usage_summary_optimism_daily_2021'
                              , 'usage_summary_optimism_daily_2022'
                              ] %}

{% set incremental_model_to_include = 'usage_summary_optimism_daily_incremental' %}


{{ base_incremental_model_union_alls(base_models_to_union, incremental_model_to_include) }}
