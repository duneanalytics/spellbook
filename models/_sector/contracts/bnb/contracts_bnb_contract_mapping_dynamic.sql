 {{
  config(     
        schema = 'contracts_bnb',
        alias = 'contract_mapping_dynamic',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['blockchain', 'contract_address'],
        partition_by =['created_month']
  )
}}

{{contracts_contract_mapping(
    chain='bnb', standard_name = 'bep'
)}}