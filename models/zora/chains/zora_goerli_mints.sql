{{ config(
        schema = 'zora_goerli',
        alias = 'mints',
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['tx_hash', 'token_id', 'evt_index']
)
}}

{{zora_mints(
    blockchain = 'goerli'
    , erc721_mints = source('zora_goerli', 'ERC721Drop_evt_Sale')
    , erc1155_mints = source('zora_goerli', 'ZoraCreator1155Impl_evt_Purchased')
)}}