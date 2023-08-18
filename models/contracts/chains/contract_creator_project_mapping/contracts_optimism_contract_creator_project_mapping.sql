 {{
  config(
        tags = ['dunesql'],
        alias = alias('contract_optimism_creator_project_mapping_'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address',
        partition_by = ['created_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{{contract_creator_project_mapping_by_chain(
    chain='optimism'
)}}