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
        transfers_rolling_hour = ref('transfers_ethereum_eth_rolling_hour'),
        filter_suicide_contracts = ref('balances_ethereum_eth_suicide'),
        filter_miner_addresses = ref('balances_ethereum_eth_miners')
    )
}}
