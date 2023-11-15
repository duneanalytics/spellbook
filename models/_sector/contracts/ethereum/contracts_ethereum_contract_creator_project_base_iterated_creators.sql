 {{
  config(
        schema = 'contracts_ethereum',
        alias = 'contract_creator_project_base_base_iterated_creators',
        materialized ='table',
        unique_key='contract_address',
        partition_by = ['created_month']
  )
}}

{{contract_creator_project_base_base_iterated_creators(
    chain='ethereum'
)}}