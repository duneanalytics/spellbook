{{ config(
    schema = 'tokens_worldchain',
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ expose_spells(
                        blockchains = \'["worldchain"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["aalan3"]\') }}'
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
