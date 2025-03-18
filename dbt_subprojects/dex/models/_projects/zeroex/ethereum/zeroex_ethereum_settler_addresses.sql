{{
    config(
        schema = 'zeroex',
        alias = 'ethereum_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Ethereum
{{ zeroex_settler_addresses('ethereum') }} 