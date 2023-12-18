 {{
  config(     
        schema = 'contracts_arbitrum',
        alias = 'contract_mapping',
        materialized ='table'
  )
}}

{{contracts_contract_mapping(
    chain='arbitrum'
)}}