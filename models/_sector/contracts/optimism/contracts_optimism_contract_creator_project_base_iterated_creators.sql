 {{
  config(
        schema = 'contracts_optimism',
        alias = 'contract_creator_project_base_iterated_creators',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address',
        partition_by = ['created_month']
  )
}}

{{contract_creator_project_base_iterated_creators(
    chain='optimism'
)}}