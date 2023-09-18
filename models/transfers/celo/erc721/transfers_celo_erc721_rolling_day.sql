{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc721_rolling_day'),
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago"]\') }}'
    )
}}

{{
    transfers_erc721_rolling_day(
        transfers_erc721_agg_day = ref('transfers_celo_erc721_agg_day')
    )
}}
