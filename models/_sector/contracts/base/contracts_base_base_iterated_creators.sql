 {{
  config(
        schema = 'contracts_base',
        alias = 'base_iterated_creators',
        materialized ='table',
        unique_key = ['blockchain', 'contract_address'],
        partition_by = ['created_month'],
  )
}}
-- depends_on: {{ ref('contracts_deterministic_contract_creators') }}

{{contracts_base_iterated_creators(
    chain='base'
)}}