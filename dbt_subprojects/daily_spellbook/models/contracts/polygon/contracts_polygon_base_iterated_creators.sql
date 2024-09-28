 {{
  config(
        schema = 'contracts_polygon',
        alias = 'base_iterated_creators',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['blockchain', 'contract_address'],
        partition_by = ['created_month']
  )
}}

{{contracts_base_iterated_creators(
    chain='polygon', days_forward=183
)}}