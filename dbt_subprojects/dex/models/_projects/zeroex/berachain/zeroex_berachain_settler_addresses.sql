{{
    config(
        schema = 'zeroex',
        alias = 'berachain_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Berachain
{{ zeroex_settler_addresses('berachain') }} 