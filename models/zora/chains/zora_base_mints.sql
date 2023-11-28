{{ config(
        schema = 'zora_base',
        alias = 'mints',
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['tx_hash', 'token_id', 'evt_index']
)
}}

{{zora_mints(
    blockchain = 'base'
    , erc721_mints = source('zora_base', 'ERC721Drop_evt_Sale')
    , erc1155_mints = source('zora_base', 'ZoraCreator1155_evt_Purchased')
)}}