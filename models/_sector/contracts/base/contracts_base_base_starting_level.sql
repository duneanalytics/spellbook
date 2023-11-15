 {{
  config(
        schema = 'contracts_base',
        alias = 'base_starting_level',
        materialized ='table',
        partition_by = ['created_month']
  )
}}

{{contracts_base_starting_level(
    chain='base'
)}}