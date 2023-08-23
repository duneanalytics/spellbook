{{ config(
        tags = ['dunesql'],
        schema = 'zora_optimism',
        alias =alias('mints'),
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_transfer_id']
)
}}

{{zora_mints(
    blockchain = 'optimism'
    , erc721_mints = source('zora_optimism', 'ERC721Drop_evt_Sale')
    , erc721_fee = source('zora_optimism', 'ERC721Drop_evt_MintFeePayout')
    , erc721_zora_transfers = source('zora_optimism', 'ERC721Drop_evt_Transfer')
    , erc1155_mints = source('zora_optimism', 'Zora1155_evt_Purchased')
    , erc1155_royalties = source('zora_optimism', 'Zora1155_evt_UpdatedRoyalties')
    , zora_protocol_rewards = source('zora_optimism', 'ProtocolRewards_call_depositRewards')
)}}