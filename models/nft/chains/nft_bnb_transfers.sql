{{ config(
        tags = ['dunesql'],
        schema = 'nft_bnb',
        alias =alias('transfers'),
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_transfer_id']
)
}}

{{nft_transfers(
    blockchain='bnb'
    , base_transactions = source('bnb','transactions')
    , erc721_transfers = source('erc721_bnb','evt_transfer')
    , erc1155_single = source('erc1155_bnb','evt_transfersingle')
    , erc1155_batch = source('erc1155_bnb', 'evt_transferbatch')
)}}
