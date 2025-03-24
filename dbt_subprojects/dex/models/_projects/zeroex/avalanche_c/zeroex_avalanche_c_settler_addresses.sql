{{
    config(
        schema = 'zeroex',
        alias = 'avalanche_c_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Avalanche C-Chain
{{ zeroex_settler_addresses('avalanche_c') }} 