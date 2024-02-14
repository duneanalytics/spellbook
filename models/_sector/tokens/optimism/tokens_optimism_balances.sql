{{ config(
        schema = 'tokens_optimism',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "tokens",
                                    \'["aalan3"]\') }}'
        )
}}

{{
    balances_enrich(
        balances_base = source('tokens_optimism', 'balances_optimism_0001'),
        blockchain = 'optimism',
    )
}}
