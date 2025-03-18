{{
    config(
        schema = 'zeroex',
        alias = 'fantom_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Fantom
{{ zeroex_settler_addresses('fantom') }} 