{{ config(
        tags = ['dunesql'],
        schema = 'nft_zksync',
        alias=alias('transfers'),
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
        unique_key = ['tx_hash', 'evt_index', 'token_id', 'amount']
)
}}

{{nft_transfers(
    blockchain='zksync'
    , base_transactions = source('zksync','transactions')
    , erc721_transfers = source('erc721_zksync','evt_transfer')
    , erc1155_single = source('erc1155_zksync','evt_transfersingle')
    , erc1155_batch = source('erc1155_zksync', 'evt_transferbatch')
)}}
