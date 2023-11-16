 {{
  config(
        schema = 'contracts_optimism',
        alias = 'base_iterated_creators',
        materialized ='table',
        partition_by = ['created_month']
  )
}}

{{contracts_base_iterated_creators(
    chain='optimism'
)}}