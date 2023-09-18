{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc1155_rolling_day'),
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago"]\') }}'
    )
}}

{{
    transfers_erc1155_rolling_day(
        transfers_erc1155_agg_day = ref('transfers_celo_erc1155_agg_day')
    )
}}
