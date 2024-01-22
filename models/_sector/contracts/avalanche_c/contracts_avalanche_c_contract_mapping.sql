 {{
  config(     
        schema = 'contracts_avalanche_c',
        alias = 'contract_mapping',
        materialized ='table',
        partition_by =['created_month']
  )
}}

{{contracts_contract_mapping(
    chain='avalanche_c'
)}}