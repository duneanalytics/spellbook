 {{
  config(
        tags = ['dunesql'],
        schema = 'contracts_base',
        alias = alias('creator_project_mapping'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address',
        partition_by = ['created_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{{contract_creator_project_mapping_by_chain(
    chain='base'
)}}