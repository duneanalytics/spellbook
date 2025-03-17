{{
    config(
        schema = 'zeroex',
        alias = 'fantom_settler_addresses',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['settler_address', 'token_id']
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Fantom
{{ zeroex_settler_addresses('fantom') }} 