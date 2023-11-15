 {{
  config(
        schema = 'contracts_optimism',
        alias = 'base_starting_level',
        materialized ='table',
        unique_key='contract_address',
        partition_by = ['created_month']
  )
}}
-- depends_on: {{ ref('contracts_deterministic_contract_creators') }}

{{contracts_base_starting_level(
    chain='optimism'
)}}