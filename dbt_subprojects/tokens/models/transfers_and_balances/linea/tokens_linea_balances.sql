{{ config(
        schema = 'tokens_linea',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ deprecate_spells() }}'
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_linea', 'balances_linea'),'linea')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
