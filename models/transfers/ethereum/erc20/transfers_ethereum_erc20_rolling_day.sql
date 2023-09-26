{{ config(
        tags = ['dunesql'],
        alias = alias('erc20_rolling_day'))
}}

{{
    transfers_erc20_rolling_day(
        transfers_erc20_agg_day = ref('transfers_ethereum_erc20_agg_day')
    )
}}
