{{ config(
        schema = 'zora_optimism',
        alias = 'mints',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'token_id', 'evt_index']
)
}}

{{zora_mints(
    blockchain = 'optimism'
    , wrapped_native_token_address = '0x4200000000000000000000000000000000000006'
    , erc721_mints = source('zora_optimism', 'ERC721Drop_evt_Sale')
    , erc1155_mints = source('zora_optimism', 'ZoraCreator1155Impl_evt_Purchased')
    , transactions = source('optimism', 'transactions')
)}}