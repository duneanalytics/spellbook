{{ config(
        tags = ['dunesql'],
        schema = 'nft_fantom',
        alias =alias('transfers'),
        partition_by='block_date',
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_transfer_id']
)
}}

{{nft_transfers(
    blockchain='fantom'
    , base_transactions = source('fantom','transactions')
    , erc721_transfers = source('erc721_fantom','evt_transfer')
    , erc1155_single = source('erc1155_fantom','evt_transfersingle')
    , erc1155_batch = source('erc1155_fantom', 'evt_transferbatch')
)}}
