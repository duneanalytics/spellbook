{{
    config(
        schema = 'zeroex',
        alias = 'worldchain_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Worldchain
{{ zeroex_settler_addresses('worldchain') }} 