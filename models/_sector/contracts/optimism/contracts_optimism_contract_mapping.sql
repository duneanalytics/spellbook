 {{
  config(     
        schema = 'contracts_optimism',
        alias = 'contract_creator_project_mapping',
        materialized ='table'
  )
}}

{{contracts_contract_mapping(
    chain='optimism'
)}}