{{
    config(
        schema = 'zeroex',
        alias = 'blast_settler_addresses',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['settler_address', 'token_id'],
        incremental_predicates = [incremental_predicate('block_time')]
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Blast
{{ zeroex_settler_addresses('blast') }} 