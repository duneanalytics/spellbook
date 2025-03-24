{{
    config(
        schema = 'zeroex',
        alias = 'linea_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Linea
{{ zeroex_settler_addresses('linea') }} 