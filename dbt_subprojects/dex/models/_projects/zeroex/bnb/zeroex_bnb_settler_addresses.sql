{{
    config(
        schema = 'zeroex',
        alias = 'bnb_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for BNB Chain
{{ zeroex_settler_addresses('bnb') }} 