{{ config(
    schema = 'soundxyz_ethereum',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)']
    )
}}


{{soundxyz_referral_rewards(blockchain = "ethereum",evt_Minted = source('sound_xyz_ethereum','RangeEditionMinter_evt_Minted'))}}
union all
{{soundxyz_referral_rewards(blockchain = "ethereum",evt_Minted = source('sound_xyz_ethereum','MerkleDropMinter_evt_Minted'))}}
union all
{{soundxyz_referral_rewards(blockchain = "ethereum",evt_Minted = source('sound_xyz_ethereum','RangeEditionMinterV2_evt_Minted'))}}
union all
{{soundxyz_referral_rewards(blockchain = "ethereum",evt_Minted = source('sound_xyz_ethereum','MerkleDropMinterV2_evt_Minted'))}}
