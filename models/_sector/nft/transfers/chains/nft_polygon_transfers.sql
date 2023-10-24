{{ config(
        
        schema = 'nft_polygon',
        alias ='transfers',
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'evt_index', 'token_id', 'amount'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
)
}}

{{nft_transfers(
    blockchain='polygon'
    , base_transactions = source('polygon','transactions')
    , erc721_transfers = source('erc721_polygon','evt_transfer')
    , erc1155_single = source('erc1155_polygon','evt_transfersingle')
    , erc1155_batch = source('erc1155_polygon', 'evt_transferbatch')
)}}
