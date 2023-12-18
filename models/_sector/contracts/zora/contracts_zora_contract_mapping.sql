 {{
  config(     
        schema = 'contracts_zora',
        alias = 'contract_mapping',
        materialized ='table'
  )
}}

{{contracts_contract_mapping(
    chain='zora'
)}}