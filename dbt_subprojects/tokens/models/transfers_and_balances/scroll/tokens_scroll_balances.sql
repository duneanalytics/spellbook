{{ config(
        schema = 'tokens_scroll',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["scroll"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["aalan3"]\') }}'
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_scroll', 'balances_scroll'), 'scroll')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
