{{ config(
        schema = 'transfers_ethereum',
        tags = ['dunesql'],
        alias = alias('erc20_rolling_hour'))
}}

{{
    transfers_erc20_rolling_hour(
        transfers_erc20_agg_hour = ref('transfers_ethereum_erc20_agg_hour')
    )
}}