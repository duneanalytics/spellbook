{{
    config(
        schema = 'zeroex',
        alias = 'mode_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Mode
{{ zeroex_settler_addresses('mode') }} 