 {{
  config(     
        schema = 'contracts_fantom',
        alias = 'contract_mapping_dynamic',
        materialized ='table',
        partition_by =['created_month']
  )
}}

{{contracts_contract_mapping(
    chain='fantom'
)}}