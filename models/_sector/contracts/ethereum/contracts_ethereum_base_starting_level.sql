 {{
  config(
        schema = 'contracts_ethereum',
        alias = 'base_starting_level',
        materialized ='table',
        partition_by = ['created_month']
  )
}}
-- depends_on: {{ ref('contracts_deterministic_contract_creators') }}

{{contracts_base_starting_level(
    chain='ethereum'
)}}