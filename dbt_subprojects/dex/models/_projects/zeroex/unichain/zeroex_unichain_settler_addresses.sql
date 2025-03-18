{{
    config(
        schema = 'zeroex',
        alias = 'unichain_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Unichain
{{ zeroex_settler_addresses('unichain') }} 