{{ config(
        tags = ['dunesql'],
        schema = 'transfers_ethereum_eth',
        alias = alias('eth_rolling_hour'))
}}

{{
    transfers_fungible_rolling_hour(
        transfers_fungible_agg_hour = ref('transfers_ethereum_eth_agg_hour')
    )
}}