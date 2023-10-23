{{ config(
        
        schema = 'nft_arbitrum',
        alias='transfers',
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
        unique_key = ['tx_hash', 'evt_index', 'token_id', 'amount']
)
}}

{{nft_transfers(
    blockchain='arbitrum'
    , base_transactions = source('arbitrum','transactions')
    , erc721_transfers = source('erc721_arbitrum','evt_transfer')
    , erc1155_single = source('erc1155_arbitrum','evt_transfersingle')
    , erc1155_batch = source('erc1155_arbitrum', 'evt_transferbatch')
)}}
