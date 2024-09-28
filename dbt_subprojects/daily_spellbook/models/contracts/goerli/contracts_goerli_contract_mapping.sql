 {{
  config(     
        schema = 'contracts_goerli',
        alias = 'contract_mapping',
        materialized ='table',
        partition_by =['created_month']
  )
}}

{{contracts_contract_mapping(
    chain='goerli'
)}}