{{ config(
    schema = 'tokens_worldchain',
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ deprecate_spells() }}'
    )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_worldchain', 'balances_worldchain'),'worldchain')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
