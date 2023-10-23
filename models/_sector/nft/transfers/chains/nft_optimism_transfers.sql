{{ config(
        
        schema = 'nft_optimism',
        alias ='transfers',
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'evt_index', 'token_id', 'amount'])
}}

{{nft_transfers(
    blockchain='optimism'
    , base_transactions = source('optimism','transactions')
    , erc721_transfers = source('erc721_optimism','evt_transfer')
    , erc1155_single = source('erc1155_optimism','evt_transfersingle')
    , erc1155_batch = source('erc1155_optimism', 'evt_transferbatch')
)}}
