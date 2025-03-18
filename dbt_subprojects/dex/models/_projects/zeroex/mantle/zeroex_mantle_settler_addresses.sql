{{
    config(
        schema = 'zeroex',
        alias = 'mantle_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Mantle
{{ zeroex_settler_addresses('mantle') }} 