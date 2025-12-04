 {{
  config(
        tags = ['prod_exclude'],
        schema = 'contracts_gnosis',
        alias = 'contract_mapping',
        materialized ='table',
        partition_by =['created_month']
  )
}}

{{contracts_contract_mapping(
    chain='gnosis'
)}}