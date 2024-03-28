{{ config(
    schema = 'dex_ethereum',
    alias = 'liquidity',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'day', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
    )
}}

{{
    dex_liquidity(
        blockchain = 'ethereum'
        , pools_model = ref('dex_ethereum_pools')
        , balances_model = ref('tokens_ethereum_balances_daily')
    )
}}