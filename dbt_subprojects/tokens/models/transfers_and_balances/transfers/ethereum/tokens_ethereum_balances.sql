{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["ethereum"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["aalan3"]\') }}'
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_ethereum', 'balances_ethereum'), 'ethereum')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
