 {{
  config(     
        schema = 'contracts_optimism',
        alias = 'contract_creator_project_mapping',
        unique_key='contract_address'
  )
}}

{{contracts_contract_mapping(
    chain='optimism'
)}}