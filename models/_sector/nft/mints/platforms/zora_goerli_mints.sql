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
    , wrapped_native_token_address = '0xb4fbf271143f4fbf7b91a5ded31805e42b2208d6'
    , erc721_mints = source('zora_goerli', 'ERC721Drop_evt_Sale')
    , erc1155_mints = source('zora_goerli', 'ZoraCreator1155Impl_evt_Purchased')
    , transactions = source('goerli', 'transactions')
)}}