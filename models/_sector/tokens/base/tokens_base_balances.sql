{{ config(
    schema = 'tokens_base',
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ expose_spells(\'["base"]\',
                                "sector",
                                "tokens",
                                \'["aalan3"]\') }}'
    )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_base', 'balances_base'),'base')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
