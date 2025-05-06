{{
    config(
        schema = 'zeroex_avalanche_c',
        alias = 'settler_addresses',
        materialized='incremental',
        unique_key = ['settler_address'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge'    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for avalanche_c
{{ zeroex_settler_addresses('avalanche_c') }} 