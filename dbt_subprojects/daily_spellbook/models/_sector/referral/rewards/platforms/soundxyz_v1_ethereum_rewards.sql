{{ config(
    schema = 'soundxyz_v1_ethereum',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}


{{soundxyz_referral_rewards(
    blockchain = "ethereum",
    evt_Minted_models = [source('sound_xyz_ethereum','RangeEditionMinter_evt_Minted')
                        ,source('sound_xyz_ethereum','MerkleDropMinter_evt_Minted')
                        ,source('sound_xyz_ethereum','RangeEditionMinterV2_evt_Minted')
                        ,source('sound_xyz_ethereum','MerkleDropMinterV2_evt_Minted')
                        ]
 )}}
