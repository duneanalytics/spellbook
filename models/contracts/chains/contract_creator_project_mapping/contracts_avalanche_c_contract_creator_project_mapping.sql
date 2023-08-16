 {{
  config(
        tags = ['dunesql'],
        alias = alias('contract_creator_project_mapping_avalanche_c'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address'
  )
}}

{{contract_creator_project_mapping_by_chain(
    chain='avalanche_c'
)}}