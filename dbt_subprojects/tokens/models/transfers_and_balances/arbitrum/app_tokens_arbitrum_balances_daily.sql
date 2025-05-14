{{ config(
        schema = 'tokens_arbitrum',
        alias = 'test_balances_daily',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["arbitrum"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["0xBoxer"]\') }}'
        )
}}

SELECT *
FROM {{ ref('tokens_arbitrum_balances_daily') }} 