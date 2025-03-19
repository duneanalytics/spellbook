{{
    config(
        schema = 'zeroex',
        alias = 'ink_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for ink
{{ zeroex_settler_addresses('ink') }} 