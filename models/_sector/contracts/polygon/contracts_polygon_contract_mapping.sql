 {{
  config(     
        schema = 'contracts_polygon',
        alias = 'contract_mapping',
        materialized ='table',
        on_table_exists = 'drop',
        partition_by =['created_month']
  )
}}

{{contracts_contract_mapping(
    chain='polygon'
)}}