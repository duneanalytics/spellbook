{{ config(
        tags = ['dunesql'],
        schema = 'nft_avalanche_c',
        alias ='transfers',
        partition_by='block_date',
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_transfer_id']
)
}}

{{nft_transfers(
    blockchain='avalanche_c'
    , base_transactions = source('avalanche_c','transactions')
    , erc721_transfers = source('erc721_avalanche_c','evt_transfer')
    , erc1155_single = source('erc1155_avalanche_c','evt_transfersingle')
    , erc1155_batch = source('erc1155_avalanche_c', 'evt_transferbatch')
)}}
