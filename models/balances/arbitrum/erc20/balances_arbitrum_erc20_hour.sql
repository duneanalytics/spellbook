{{ config(
        tags = ['dunesql'],
        schema = 'balances_arbitrum',
        alias = alias('erc20_hour'),
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}

{{
    balances_fungible_hour(
        blockchain = 'arbitrum',
        first_transaction_date = '2021-05-29',
        is_more_than_year_ago = true,
        transfers_rolling_hour = ref('transfers_arbitrum_erc20_rolling_hour'),
        balances_noncompliant = ref('balances_arbitrum_erc20_noncompliant')
    )
}}