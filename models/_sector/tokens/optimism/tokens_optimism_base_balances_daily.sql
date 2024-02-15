{{ config(
        schema = 'tokens_optimism',
        alias = 'base_balances_daily',
        file_format = 'delta',
        materialized='incremental',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['unique_key'],
        )
}}

{{
    balances_daily(
        balances_base = source('tokens_optimism', 'balances_optimism_0001')
    )
}}
