 {{
  config(     
        schema = 'contracts_bnb',
        alias = 'contract_mapping_dynamic',
        materialized ='table',
        partition_by =['created_month']
  )
}}

{{contracts_contract_mapping(
    chain='bnb', standard_name = 'bep'
)}}