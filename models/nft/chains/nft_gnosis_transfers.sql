{{ config(
        tags = ['dunesql'],
        schema = 'nft_gnosis',
        alias =alias('transfers'),
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_transfer_id']
)
}}

{{nft_transfers(
    blockchain='gnosis'
    , base_transactions = source('gnosis','transactions')
    , erc721_transfers = source('erc721_gnosis','evt_transfer')
    , erc1155_single = source('erc1155_gnosis','evt_transfersingle')
    , erc1155_batch = source('erc1155_gnosis', 'evt_transferbatch')
)}}
