{{ config(
        
        alias = 'erc20_rolling_hour')
}}

{{
    transfers_erc20_rolling_hour(
        transfers_erc20_agg_hour = ref('transfers_gnosis_erc20_agg_hour')
    )
}}