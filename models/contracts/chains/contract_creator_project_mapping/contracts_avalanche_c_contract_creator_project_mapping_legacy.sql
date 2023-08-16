 {{
  config(
        tags = ['legacy'],
        alias = alias('contract_creator_project_mapping_avalanche_c',legacy_model=True),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address'
  )
}}

SELECT 1