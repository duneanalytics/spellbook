{{ config(
        schema = 'balances_ethereum_eth',
        tags = ['dunesql'],
        alias = alias('eth_latest'),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}


{{
    balances_fungible_latest(
        blockchain = 'ethereum',
        transfers_rolling_hour = ref('transfers_ethereum_eth_rolling_hour'),
        filter_suicide_contracts = ref('balances_ethereum_eth_suicide'),
        filter_miner_addresses = ref('balances_ethereum_eth_miners')
    )
}}