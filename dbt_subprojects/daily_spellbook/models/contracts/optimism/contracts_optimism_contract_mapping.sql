 {{
  config(     
        schema = 'contracts_optimism',
        alias = 'contract_creator_project_mapping',
        materialized ='table',
        partition_by =['created_month']
  )
}}

{{contracts_contract_mapping(
    chain='optimism'
)}}