{{ config(
        schema = 'zora_goerli',
        alias = 'mints',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'token_id', 'evt_index']
)
}}

{{zora_mints(
    blockchain = 'goerli'
    , erc721_mints = source('zora_goerli', 'ERC721Drop_evt_Sale')
    , erc1155_mints = source('zora_goerli', 'ZoraCreator1155Impl_evt_Purchased')
    , transactions = tef('goerli', 'transactions')
)}}