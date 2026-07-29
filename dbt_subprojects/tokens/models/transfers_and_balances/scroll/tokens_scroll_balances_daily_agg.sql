{{ config(
        schema = 'tokens_scroll',
        alias = 'balances_daily_agg',
        materialized = 'view',
        post_hook = '{{ deprecate_spells() }}'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_scroll_balances_daily_agg_base'),
        daily=true,
    )
}}
