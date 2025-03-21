{{
    config(
        schema = 'zeroex_mode',
        alias = 'settler_addresses',
        materialized='incremental',
        partition_by = ['begin_block_time'],
        unique_key = ['token_id', 'settler_address', 'begin_block_number', 'begin_block_time'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.begin_block_time')]
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for mode
{{ zeroex_settler_addresses('mode') }} 