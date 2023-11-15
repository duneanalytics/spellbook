 {{
  config(     
        schema = 'contracts_base',
        alias = 'contract_mapping',
        unique_key='contract_address'
  )
}}

{{contracts_contract_mapping(
    chain='base'
)}}