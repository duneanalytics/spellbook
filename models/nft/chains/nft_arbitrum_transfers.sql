{{ config(
        tags = ['dunesql'],
        schema = 'nft_arbitrum',
        alias=alias('transfers'),
        partition_by='block_date',
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_transfer_id']
)
}}

{{nft_transfers(
    blockchain='arbitrum'
    , base_transactions = source('arbitrum','transactions')
    , erc721_transfers = source('erc721_arbitrum','evt_transfer')
    , erc1155_single = source('erc1155_arbitrum','evt_transfersingle')
    , erc1155_batch = source('erc1155_arbitrum', 'evt_transferbatch')
)}}
