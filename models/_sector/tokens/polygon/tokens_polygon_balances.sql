{{ config(
        schema = 'tokens_polygon',
        alias = 'balances',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_base = source('tokens_polygon', 'balances_polygon_0001'),
        blockchain = 'polygon',
    )
}}
