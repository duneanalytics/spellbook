{{ config(
    schema = 'soundxyz_v1_optimism',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{soundxyz_referral_rewards(
    blockchain = "optimism",
    evt_Minted_models = [source('sound_xyz_optimism','RangeEditionMinter_evt_Minted')
                        ,source('sound_xyz_optimism','MerkleDropMinterV2_evt_Minted')
                        ,source('sound_xyz_optimism','RangeEditionMinterV2_evt_Minted')
                        ,source('sound_xyz_optimism','MerkleDropMinterV2_1_evt_Minted')
                        ]
 )}}
