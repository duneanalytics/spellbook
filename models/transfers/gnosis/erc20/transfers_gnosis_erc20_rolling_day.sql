{{ config(
        
        alias = 'erc20_rolling_day')
}}

{{
    transfers_erc20_rolling_day(
        transfers_erc20_agg_day = ref('transfers_gnosis_erc20_agg_day')
    )
}}