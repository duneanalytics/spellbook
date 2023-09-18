{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc1155_agg_day'),
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
    transfers_erc1155_agg_day(
        transfers_erc1155 = ref('transfers_celo_erc1155')
    )
}}
