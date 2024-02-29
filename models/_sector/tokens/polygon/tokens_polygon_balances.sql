{{ config(
        schema = 'tokens_polygon',
        alias = 'balances',
        materialized = 'view'
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_polygon', 'balances_polygon_0001'),'polygon')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
