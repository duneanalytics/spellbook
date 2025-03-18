{{
    config(
        schema = 'zeroex',
        alias = 'base_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Base
{{ zeroex_settler_addresses('base') }} 