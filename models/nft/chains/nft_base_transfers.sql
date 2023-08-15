{{ config(
        tags = ['dunesql'],
        schema = 'nft_base',
        alias =alias('transfers'),
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_transfer_id']
)
}}

{{nft_transfers(
    blockchain='base'
    , base_transactions = source('base','transactions')
    , erc721_transfers = source('erc721_base','evt_transfer')
    , erc1155_single = source('erc1155_base','evt_transfersingle')
    , erc1155_batch = source('erc1155_base', 'evt_transferbatch')
)}}
