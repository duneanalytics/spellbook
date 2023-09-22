{{ config(
        schema = 'balances_ethereum',
        tags = ['dunesql'],
        alias = alias('eth_day'),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}


{{
    balances_fungible_day(
        blockchain = 'ethereum',
        first_transaction_date = '2015-08-07',
        transfers_rolling_day = ref('transfers_ethereum_eth_rolling_day'),
        filter_suicide_contracts = ref('balances_ethereum_eth_suicide'),
        filter_miner_addresses = ref('balances_ethereum_eth_miners')
    )
}}
