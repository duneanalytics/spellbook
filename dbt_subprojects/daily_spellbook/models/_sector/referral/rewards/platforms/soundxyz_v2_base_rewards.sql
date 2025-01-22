{{ config(
    schema = 'soundxyz_v2_base',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{soundxyz_v2_referral_rewards(
        blockchain = "base"
        ,SuperMinterV2_evt_Minted = source('sound_xyz_base','SuperMinterV2_evt_Minted')
        )
    }}
