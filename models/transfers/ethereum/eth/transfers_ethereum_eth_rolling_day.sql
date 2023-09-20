{{ config(
        tags = ['dunesql'],
        schema = 'transfers_ethereum_eth',
        alias = alias('eth_rolling_day'))
}}


{{
    transfers_fungible_rolling_day(
        transfers_fungible_agg_day = ref('transfers_ethereum_eth_agg_day')
    )
}}