 {{
  config(     
        schema = 'contracts_ethereum',
        alias = 'contract_mapping',
        materialized ='table'
  )
}}

{{contracts_contract_mapping(
    chain='ethereum'
)}}