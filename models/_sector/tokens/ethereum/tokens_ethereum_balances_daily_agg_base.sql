{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances_daily_agg',
        file_format = 'delta',
        materialized='incremental',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['unique_key'],
        )
}}

{{
    balances_daily_agg(
        balances_base = source('tokens_ethereum', 'balances_ethereum_0004')
    )
}}
