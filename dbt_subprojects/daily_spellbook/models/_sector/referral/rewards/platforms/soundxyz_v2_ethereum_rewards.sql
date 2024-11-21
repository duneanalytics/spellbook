{{ config(
    schema = 'soundxyz_v2_ethereum',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{soundxyz_v2_referral_rewards(
        blockchain = "ethereum"
        ,SuperMinterV2_evt_Minted = source('sound_xyz_ethereum','SuperMinterV2_evt_Minted')
        )
    }}
