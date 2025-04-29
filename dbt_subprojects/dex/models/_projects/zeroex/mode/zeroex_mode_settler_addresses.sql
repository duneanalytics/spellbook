{{
    config(
        schema = 'zeroex_mode',
        alias = 'settler_addresses',
        materialized='incremental',
        unique_key = ['settler_address'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge'    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for mode
{{ zeroex_settler_addresses('mode') }} 