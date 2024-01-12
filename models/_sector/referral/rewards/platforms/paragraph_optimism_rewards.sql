{{ config(
    schema = 'paragraph_optimism',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{paragraph_referral_rewards(
        blockchain = "optimism"
        ,FeeManager_evt_FeeDistributed = source('paragraph_optimism','FeeManager_evt_FeeDistributed')
        )
    }}
