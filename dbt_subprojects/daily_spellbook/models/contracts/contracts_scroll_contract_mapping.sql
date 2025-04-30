{{
  config(     
        schema = 'contracts_scroll',
        alias = 'contract_mapping',
        materialized ='table',
        partition_by =['created_month']
  )
}}

{{contracts_contract_mapping(
    chain='scroll'
)}} 
