{{ config(
        tags = ['dunesql'],
        schema = 'transfers_arbitrum_erc20',
        alias = alias('erc20_rolling_day'))
}}

{{
    transfers_erc20_rolling_day(
        transfers_erc20_agg_day = ref('transfers_arbitrum_erc20_agg_day')
    )
}}