{{ config(
        tags = ['dunesql'],
        schema = 'balances_arbitrum_erc20',
        alias = alias('erc20_day'),
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}

{{
    balances_fungible_day(
        blockchain = 'arbitrum',
        first_transaction_date = '2021-05-29',
        transfers_rolling_day = ref('transfers_arbitrum_erc20_rolling_day'),
        balances_noncompliant = ref('balances_arbitrum_erc20_noncompliant')
    )
}}

