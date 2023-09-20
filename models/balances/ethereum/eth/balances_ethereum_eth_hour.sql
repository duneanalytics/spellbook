{{ config(
        schema = 'balances_ethereum_eth',
        tags = ['dunesql'],
        alias = alias('eth_hour'),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}

{{
    balances_fungible_hour(
        blockchain = 'ethereum',
        first_transaction_date = '2015-08-07',
        is_more_than_year_ago = true,
        transfers_rolling_hour = ref('transfers_ethereum_eth_rolling_hour')
    )
}}
