{{ config(
        schema = 'tokens_base',
        alias = 'balances_daily_agg_base',
        materialized='view'
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_base', 'balances_base'), 'base')}}
)

{{
    balances_daily_agg(
        balances_raw = 'balances_raw'
    )
}}
