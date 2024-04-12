{{ config(
        schema = 'zora_ethereum',
        alias = 'mints',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'token_id', 'evt_index']
)
}}

{{zora_mints(
    blockchain = 'ethereum'
    , wrapped_native_token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    , erc721_mints = source('zora_ethereum', 'ERC721Drop_evt_Sale')
    , erc1155_mints = source('zora_ethereum', 'Zora1155_evt_Purchased')
    , transactions = source('ethereum', 'transactions')
)}}