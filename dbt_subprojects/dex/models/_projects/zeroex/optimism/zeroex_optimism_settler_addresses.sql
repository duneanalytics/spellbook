{{
    config(
        schema = 'zeroex',
        alias = 'optimism_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Optimism
{{ zeroex_settler_addresses('optimism') }} 