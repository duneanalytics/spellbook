 {{
  config(
        schema = 'contracts_base',
        alias = 'contract_creator_project_base_starting_level',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address',
        partition_by = ['created_month']
  )
}}

{{contract_creator_project_base_starting_level(
    chain='base'
)}}