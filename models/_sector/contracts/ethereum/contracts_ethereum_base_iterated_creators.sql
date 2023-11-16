 {{
  config(
        schema = 'contracts_ethereum',
        alias = 'base_iterated_creators',
        materialized ='table',
        unique_key = ['blockchain', 'contract_address', 'created_tx_hash'],
        partition_by = ['created_month']
  )
}}

{{contracts_base_iterated_creators(
    chain='ethereum'
)}}