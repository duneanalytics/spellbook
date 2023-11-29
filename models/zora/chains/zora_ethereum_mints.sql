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
    , erc721_mints = source('zora_ethereum', 'ERC721Drop_evt_Sale')
    , erc1155_mints = source('zora_ethereum', 'Zora1155_evt_Purchased')
    , transactions = tef('ethereum', 'transactions')
)}}