{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "tokens",
                                    \'["aalan3"]\') }}'
        )
}}

{{
    balances_enrich(
        balances_base = source('tokens_ethereum', 'balances_ethereum_0004'),
        blockchain = 'ethereum',
    )
}}
