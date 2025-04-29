{{
    config(
        schema = 'zeroex_scroll',
        alias = 'settler_addresses',
        materialized='incremental',
        unique_key = ['settler_address'],
        file_format ='delta',
        incremental_strategy='merge'    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for scroll
{{ zeroex_settler_addresses('scroll') }} 