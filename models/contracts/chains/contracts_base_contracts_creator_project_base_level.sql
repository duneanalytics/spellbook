 {{
  config(
        tags = ['dunesql'],
        schema = 'contracts_base',
        alias = alias('creator_project_base_level'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address',
        partition_by = ['created_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{{contracts_creator_project_base_level(
    chain='base'
)}}