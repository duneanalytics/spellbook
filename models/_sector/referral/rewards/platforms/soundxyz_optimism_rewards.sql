{{ config(
    schema = 'soundxyz_optimism',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'view'
    )
}}

WITH rewards_cte as (
    {{soundxyz_referral_rewards(blockchain = "optimism",evt_Minted = source('sound_xyz_optimism','RangeEditionMinter_evt_Minted'))}}
    union all
    {{soundxyz_referral_rewards(blockchain = "optimism",evt_Minted = source('sound_xyz_optimism','MerkleDropMinterV2_evt_Minted'))}}
    union all
    {{soundxyz_referral_rewards(blockchain = "optimism",evt_Minted = source('sound_xyz_optimism','RangeEditionMinterV2_evt_Minted'))}}
    union all
    {{soundxyz_referral_rewards(blockchain = "optimism",evt_Minted = source('sound_xyz_optimism','MerkleDropMinterV2_1_evt_Minted'))}}
)

{{ expand_referral_rewards(
    blockchain='optimism'
    ,rewards_cte='rewards_cte') }}
