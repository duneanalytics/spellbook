{{ config(
        schema = 'zora_base',
        alias = 'mints',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'token_id', 'evt_index']
)
}}

{{zora_mints(
    blockchain = 'base'
    , wrapped_native_token_address = '0x4200000000000000000000000000000000000006'
    , erc721_mints = source('zora_base', 'ERC721Drop_evt_Sale')
    , erc1155_mints = source('zora_base', 'ZoraCreator1155_evt_Purchased')
    , transactions = source('base', 'transactions')
)}}