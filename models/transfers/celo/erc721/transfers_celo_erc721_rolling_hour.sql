{{ 
    config(
        tags = ['dunesql'],
        schema = 'transfers_celo',
        alias = alias('erc721_rolling_hour'),
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago"]\') }}'
    )
}}

{{
    transfers_erc721_rolling_hour(
        transfers_erc721_agg_hour = ref('transfers_celo_erc721_agg_hour')
    )
}}
