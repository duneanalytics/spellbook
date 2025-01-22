{{ config(
        schema = 'tokens_polygon',
        alias = 'balances_daily_agg',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_polygon_balances_daily_agg_base'),
        daily=true,
    )
}}
