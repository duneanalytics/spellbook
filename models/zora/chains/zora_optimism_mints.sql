{{ config(
        tags = ['dunesql'],
        schema = 'zora_optimism',
        alias =alias('mints'),
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['block_number', 'tx_hash', 'evt_index']
)
}}

{{zora_mints(
    blockchain = 'optimism'
    , erc721_mints = source('zora_optimism', 'ERC721Drop_evt_Sale')
    , erc721_fee = source('zora_optimism', 'ERC721Drop_evt_MintFeePayout')
    , erc721_zora_transfers = source('zora_optimism', 'ERC721Drop_evt_Transfer')
    , erc1155_mints = source('zora_optimism', 'ZoraCreator1155Impl_evt_Purchased')
    , erc1155_royalties = source('zora_optimism', 'ZoraCreator1155Impl_evt_UpdatedRoyalties')
    , zora_protocol_rewards = source('zora_optimism', 'ProtocolRewards_call_depositRewards')
)}}