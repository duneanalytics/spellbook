{{
    config(
        schema = 'metric_avalanche_c',
        alias = 'pool_creations',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'pool'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')]
    )
}}

{{
    metric_compatible_pool_creations(
        blockchain = 'avalanche_c',
        project = 'metric',
        project_start_date = '2026-02-23'
    )
}}
