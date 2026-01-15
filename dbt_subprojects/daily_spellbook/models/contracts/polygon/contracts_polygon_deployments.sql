{{
  config(
        schema = 'contracts_polygon',
        alias = 'deployments',
        materialized = 'incremental',
        file_format = 'delta',
        partition_by = ['creation_block_month'],
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')]
  )
}}

{{
    contracts_deployments(blockchain='polygon')
}}
