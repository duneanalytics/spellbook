{{ config(
        schema = 'tokens_base',
        alias = 'balances_daily',
        file_format = 'delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['unique_key'],
        -- TODO: Add post_hook to expose_spells
        )
}}

{{
    balances_daily(
        balances_base = source('tokens_base', 'balances_base_daily'),
    )
}}
