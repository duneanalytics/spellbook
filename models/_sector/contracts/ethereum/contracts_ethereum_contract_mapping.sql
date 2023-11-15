 {{
  config(     
        schema = 'contracts_ethereum',
        alias = 'contract_mapping',
        unique_key='contract_address'
  )
}}

{{contracts_contract_mapping(
    chain='ethereum'
)}}