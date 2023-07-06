{{ config(
        tags = ['dunesql'],
        schema = 'nft_polygon',
        alias =alias('transfers'),
        partition_by='block_date',
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_transfer_id']
)
}}

{{nft_transfers(
    blockchain='polygon'
    , base_transactions = source('polygon','transactions')
    , erc721_transfers = source('erc721_polygon','evt_transfer')
    , erc1155_single = source('erc1155_polygon','evt_transfersingle')
    , erc1155_batch = source('erc1155_polygon', 'evt_transferbatch')
)}}
