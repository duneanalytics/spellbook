{{
    config(
        schema = 'zeroex_worldchain',
        alias = 'settler_addresses',
        materialized='incremental',
        unique_key = ['settler_address'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge'    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for worldchain
{{ zeroex_settler_addresses('worldchain') }} 