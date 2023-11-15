 {{
  config(
        schema = 'contracts_ethereum',
        alias = 'base_iterated_creators',
        materialized ='table',
        unique_key='contract_address',
        partition_by = ['created_month']
  )
}}

{{contracts_base_iterated_creators(
    chain='ethereum'
)}}