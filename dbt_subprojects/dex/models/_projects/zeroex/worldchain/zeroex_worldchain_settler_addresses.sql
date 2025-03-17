{{
    config(
        schema = 'zeroex',
        alias = 'worldchain_settler_addresses',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['settler_address', 'token_id']
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Worldchain
{{ zeroex_settler_addresses('worldchain') }} 