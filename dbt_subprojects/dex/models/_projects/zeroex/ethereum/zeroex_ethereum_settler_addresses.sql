{{
    config(
        schema = 'zeroex',
        alias = 'ethereum_settler_addresses',
        materialized='incremental',
        partition_by = ['begin_block_time'],
        unique_key = ['token_id', 'settler_address', 'begin_block_number', 'begin_block_time'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for ethereum
{{ zeroex_settler_addresses('ethereum') }} 