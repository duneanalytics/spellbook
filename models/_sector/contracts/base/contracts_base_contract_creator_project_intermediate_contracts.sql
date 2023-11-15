 {{
  config(
        schema = 'contracts_base',
        alias = 'contract_creator_project_intermediate_contracts',
        materialized ='table',
        unique_key='contract_address',
        partition_by = ['created_month']
  )
}}

{{contract_creator_project_intermediate_contracts(
    chain='base'
)}}