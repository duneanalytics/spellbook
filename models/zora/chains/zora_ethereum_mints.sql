{{ config(
        tags = ['dunesql'],
        schema = 'zora_ethereum',
        alias =alias('mints'),
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['tx_hash', 'evt_index']
)
}}

{{zora_mints(
    blockchain = 'ethereum'
    , erc721_mints = source('zora_ethereum', 'ERC721Drop_evt_Sale')
    , erc721_fee = source('zora_ethereum', 'ERC721Drop_evt_MintFeePayout')
    , erc721_zora_transfers = source('zora_ethereum', 'ERC721Drop_evt_Transfer')
    , erc1155_mints = source('zora_ethereum', 'Zora1155_evt_Purchased')
    , erc1155_royalties = source('zora_ethereum', 'Zora1155_evt_UpdatedRoyalties')
    , zora_protocol_rewards = source('zora_ethereum', 'ProtocolRewards_call_depositRewards')
)}}