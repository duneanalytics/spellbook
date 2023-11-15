 {{
  config(     
        schema = 'contracts_optimism',
        alias = 'contract_creator_project_mapping',
        unique_key='contract_address'
  )
}}

{{contract_creator_project_mapping_by_chain(
    chain='optimism'
)}}