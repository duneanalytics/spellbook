{{
    config(
        schema = 'zeroex',
        alias = 'scroll_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Scroll
{{ zeroex_settler_addresses('scroll') }} 