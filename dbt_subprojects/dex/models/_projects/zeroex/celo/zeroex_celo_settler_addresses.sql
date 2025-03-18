{{
    config(
        schema = 'zeroex',
        alias = 'celo_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Celo
{{ zeroex_settler_addresses('celo') }} 