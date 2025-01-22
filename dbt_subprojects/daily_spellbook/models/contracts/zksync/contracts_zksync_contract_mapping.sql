 {{
  config(     
        schema = 'contracts_zksync',
        alias = 'contract_mapping',
        materialized ='table',
        partition_by =['created_month']
  )
}}

{{contracts_contract_mapping(
    chain='zksync'
)}}