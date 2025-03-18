{{
    config(
        schema = 'zeroex',
        alias = 'arbitrum_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Arbitrum
{{ zeroex_settler_addresses('arbitrum') }} 