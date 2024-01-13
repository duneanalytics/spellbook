 {{
  config(     
        schema = 'contracts_arbitrum',
        alias = 'contract_mapping',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['blockchain', 'contract_address'],
        partition_by =['created_month']
  )
}}

{{contracts_contract_mapping(
    chain='arbitrum'
)}}