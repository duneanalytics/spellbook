 {{
  config(     
        schema = 'contracts_base',
        alias = 'creator_project_mapping',
        unique_key='contract_address'
  )
}}

{{contract_creator_project_mapping_by_chain(
    chain='base'
)}}