 {{
  config(     
        schema = 'contracts_bnb',
        alias = 'contract_mapping_dynamic'
  )
}}

{{contracts_contract_mapping(
    chain='bnb', standard_name = 'bep'
)}}