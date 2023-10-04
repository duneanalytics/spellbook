{{ config(
    schema = 'soundxyz_ethereum',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'view'
    )
}}

WITH rewards_cte as (
    {{soundxyz_referral_rewards(blockchain = "ethereum",evt_Minted = source('sound_xyz_ethereum','RangeEditionMinter_evt_Minted'))}}
    union all
    {{soundxyz_referral_rewards(blockchain = "ethereum",evt_Minted = source('sound_xyz_ethereum','MerkleDropMinter_evt_Minted'))}}
    union all
    {{soundxyz_referral_rewards(blockchain = "ethereum",evt_Minted = source('sound_xyz_ethereum','RangeEditionMinterV2_evt_Minted'))}}
    union all
    {{soundxyz_referral_rewards(blockchain = "ethereum",evt_Minted = source('sound_xyz_ethereum','MerkleDropMinterV2_evt_Minted'))}}
)

{{ expand_referral_rewards(
    blockchain='ethereum'
    ,rewards_cte='rewards_cte') }}
