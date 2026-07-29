{{ config(
        schema = 'tokens_optimism',
        alias = 'balances_daily_agg',
        materialized = 'view',
        post_hook = '{{ deprecate_spells() }}'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_optimism_balances_daily_agg_base'),
        daily=true,
    )
}}
