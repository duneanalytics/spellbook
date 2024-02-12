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

{{
    balances_enrich(
        balances_base = source('tokens_base', 'balances_base_0001'),
        blockchain = 'base',
    )
}}
