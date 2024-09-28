 {{
  config(
        schema = 'contracts_bnb',
        alias = 'base_iterated_creators',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['blockchain', 'contract_address'],
        partition_by = ['created_month']
  )
}}

{{contracts_base_iterated_creators(
    chain='bnb', standard_name = 'bep', days_forward=183
)}}