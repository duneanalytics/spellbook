{{ config(
        schema = 'zora_optimism',
        alias = 'mints',
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['tx_hash', 'token_id', 'evt_index']
)
}}

{{zora_mints(
    blockchain = 'optimism'
    , erc721_mints = source('zora_optimism', 'ERC721Drop_evt_Sale')
    , erc1155_mints = source('zora_optimism', 'ZoraCreator1155Impl_evt_Purchased')
)}}