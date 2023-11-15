 {{
  config(
        schema = 'contracts_optimism',
        alias = 'base_iterated_creators',
        materialized ='table',
        unique_key='contract_address',
        partition_by = ['created_month']
  )
}}

{{contracts_base_iterated_creators(
    chain='optimism'
)}}