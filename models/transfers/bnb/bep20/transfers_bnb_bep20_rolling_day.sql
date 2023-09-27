{{ config(
        tags = ['dunesql'],
        schema = 'transfers_bnb',
        alias = alias('bep20_rolling_day'))
}}

{{
    transfers_erc20_rolling_day(
        transfers_erc20_agg_day = ref('transfers_bnb_bep20_agg_day')
    )
}}