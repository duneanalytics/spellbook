{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc1155_rolling_hour'),
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago"]\') }}'
    )
}}

{{
    transfers_erc1155_rolling_hour(
        transfers_erc1155_agg_hour = ref('transfers_celo_erc1155_agg_hour')
    )
}}
