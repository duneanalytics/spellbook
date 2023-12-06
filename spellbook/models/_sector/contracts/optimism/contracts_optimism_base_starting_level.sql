 {{
  config(
        schema = 'contracts_optimism',
        alias = 'base_starting_level',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['blockchain', 'contract_address'],
        partition_by = ['created_month']
  )
}}
-- depends_on: {{ ref('contracts_deterministic_contract_creators') }}

{{contracts_base_starting_level(
    chain='optimism'
)}}