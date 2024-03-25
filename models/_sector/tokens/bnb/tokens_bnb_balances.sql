{{ config(
        schema = 'tokens_bnb',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["bnb"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["aalan3"]\') }}'
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_bnb', 'balances_bnb'), 'bnb')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
