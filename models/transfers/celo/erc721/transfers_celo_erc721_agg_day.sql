{{ 
    config(
        tags = ['dunesql'],
        schema = 'transfers_celo',
        alias = alias('erc721_agg_day'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['wallet_address', 'token_address', 'block_day', 'token_id'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago"]\') }}'
    )
}}

{{
    transfers_erc721_agg_day(
        transfers_erc721 = ref('transfers_celo_erc721')
    )
}}
