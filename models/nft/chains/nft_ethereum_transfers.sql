{{ config(
        tags = ['dunesql'],
        schema = 'nft_ethereum',
        alias =alias('transfers'),
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_transfer_id']
)
}}

{{nft_transfers(
    blockchain='ethereum'
    , base_transactions = source('ethereum','transactions')
    , erc721_transfers = source('erc721_ethereum','evt_transfer')
    , erc1155_single = source('erc1155_ethereum','evt_transfersingle')
    , erc1155_batch = source('erc1155_ethereum', 'evt_transferbatch')
)}}
