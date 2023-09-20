{{ config(
        tags = ['dunesql'],
        schema = 'transfers_bnb_bep20',
        alias = alias('bep20_rolling_hour'))
}}

{{
    transfers_erc20_rolling_hour(
        transfers_erc20_agg_hour = ref('transfers_bnb_bep20_agg_hour')
    )
}}