{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc1155'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'transfer_type', 'wallet_address', 'token_address', 'token_id', 'amount'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago"]\') }}'
    )
}}

{{
    transfers_erc1155(
        blockchain = 'celo',
        erc1155_evt_transfer_batch = source('erc1155_celo', 'evt_transferbatch'),
        erc1155_evt_transfer_single = source('erc1155_celo', 'evt_transfersingle')
    )
}}
