{{ config(
        schema = 'tokens_scroll',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["scroll"]\',
                                    "sector",
                                    "tokens",
                                    \'["aalan3"]\') }}'
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
