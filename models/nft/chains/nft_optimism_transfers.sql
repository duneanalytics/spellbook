{{ config(
        tags = ['dunesql'],
        schema = 'nft_optimism',
        alias =alias('transfers'),
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_transfer_id']
)
}}

{{nft_transfers(
    blockchain='optimism'
    , base_transactions = source('optimism','transactions')
    , erc721_transfers = source('erc721_optimism','evt_transfer')
    , erc1155_single = source('erc1155_optimism','evt_transfersingle')
    , erc1155_batch = source('erc1155_optimism', 'evt_transferbatch')
)}}
