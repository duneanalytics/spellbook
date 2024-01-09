 {{
  config(     
        schema = 'contracts_ethereum',
        alias = 'contract_mapping',
        materialized ='table',
        on_table_exists = 'drop',
        partition_by =['created_month']
  )
}}

{{contracts_contract_mapping(
    chain='ethereum'
)}}