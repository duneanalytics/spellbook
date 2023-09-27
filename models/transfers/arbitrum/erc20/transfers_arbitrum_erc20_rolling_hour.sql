{{ config(
        tags = ['dunesql'],
        schema = 'transfers_arbitrum',
        alias = alias('erc20_rolling_hour'))
}}

{{
    transfers_erc20_rolling_hour(
        transfers_erc20_agg_hour = ref('transfers_arbitrum_erc20_agg_hour')
    )
}}