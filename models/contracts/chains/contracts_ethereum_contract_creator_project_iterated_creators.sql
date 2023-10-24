 {{
  config(
        tags = ['dunesql'],
        schema = 'contracts_ethereum',
        alias = alias('contract_creator_project_iterated_creators'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address',
        partition_by = ['created_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{{contract_creator_project_iterated_creators(
    chain='ethereum'
)}}