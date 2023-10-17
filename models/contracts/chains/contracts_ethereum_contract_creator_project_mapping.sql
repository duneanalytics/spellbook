 {{
  config(
        tags = ['dunesql', 'prod_daily'],
        schema = 'contracts_ethereum',
        alias = alias('creator_project_mapping'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.created_time')],
        unique_key='contract_address',
        partition_by = ['created_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{{contract_creator_project_mapping_by_chain(
    chain='ethereum'
)}}