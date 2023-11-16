 {{
  config(
        schema = 'contracts_ethereum',
        alias = 'base_starting_level',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key=['contract_address','created_tx_hash','created_tx_index'],
        partition_by = ['created_month']
  )
}}
-- depends_on: {{ ref('contracts_deterministic_contract_creators') }}

{{contracts_base_starting_level(
    chain='ethereum'
)}}