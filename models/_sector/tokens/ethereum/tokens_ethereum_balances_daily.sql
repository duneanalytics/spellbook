{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_ethereum_balances_daily_agg'),
    start_date = '2015-07-30',
)
}}


