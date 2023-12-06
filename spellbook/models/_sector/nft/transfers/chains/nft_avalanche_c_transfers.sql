{{ config(
        
        schema = 'nft_avalanche_c',
        alias ='transfers',
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'evt_index', 'token_id', 'amount']
)
}}

{{nft_transfers(
    blockchain='avalanche_c'
    , base_transactions = source('avalanche_c','transactions')
    , erc721_transfers = source('erc721_avalanche_c','evt_transfer')
    , erc1155_single = source('erc1155_avalanche_c','evt_transfersingle')
    , erc1155_batch = source('erc1155_avalanche_c', 'evt_transferbatch')
)}}
