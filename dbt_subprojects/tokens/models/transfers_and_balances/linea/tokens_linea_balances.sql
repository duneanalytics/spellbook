{{ config(
        schema = 'tokens_linea',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["linea"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["aalan3"]\') }}'
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
