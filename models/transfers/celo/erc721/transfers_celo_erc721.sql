{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc721'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'transfer_type', 'wallet_address', 'token_address', 'token_id'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz", "tomfutago"]\') }}'
    )
}}

{{
    transfers_erc721(
        blockchain = 'celo',
        erc721_evt_transfer = source('erc721_celo', 'evt_transfer')
    )
}}
