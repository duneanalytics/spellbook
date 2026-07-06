{{  config(
    schema = 'zeroex_v2_optimism',
    alias = 'trades',
    materialized='incremental',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'block_date', 'tx_hash', 'evt_index', 'trace_address'],
    on_schema_change='sync_all_columns',
    file_format ='delta',
    incremental_strategy='merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}

{{
    zeroex_settler_agg(
        blockchain = 'optimism'
    )
}}
