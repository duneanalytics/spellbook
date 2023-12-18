 {{
  config(     
        schema = 'contracts_base',
        alias = 'contract_mapping',
        materialized ='table'
  )
}}

{{contracts_contract_mapping(
    chain='base'
)}}