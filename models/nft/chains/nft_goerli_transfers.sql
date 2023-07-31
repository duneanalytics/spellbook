{{ config(
        tags = ['dunesql'],
        schema = 'nft_goerli',
        alias =alias('transfers'),
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_transfer_id']
)
}}

{{nft_transfers(
    blockchain='goerli'
    , base_transactions = source('goerli','transactions')
    , erc721_transfers = source('erc721_goerli','evt_transfer')
    , erc1155_single = source('erc1155_goerli','evt_transfersingle')
    , erc1155_batch = source('erc1155_goerli', 'evt_transferbatch')
)}}
