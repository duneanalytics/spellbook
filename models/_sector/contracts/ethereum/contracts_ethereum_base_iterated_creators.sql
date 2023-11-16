 {{
  config(
        schema = 'contracts_ethereum',
        alias = 'base_iterated_creators',
        materialized ='table',
        partition_by = ['created_month']
  )
}}

{{contracts_base_iterated_creators(
    chain='ethereum'
)}}