{{
    config(
        schema = 'metric_base',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    metric_compatible_trades(
        blockchain = 'base',
        project = 'metric',
        project_start_date = '2026-02-23'
    )
}}
