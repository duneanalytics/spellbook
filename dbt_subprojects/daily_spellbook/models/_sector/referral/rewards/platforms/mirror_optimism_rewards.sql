{{ config(
    schema = 'mirror_optimism',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{mirror_referral_rewards(
        blockchain = "optimism"
        ,WritingEditions_evt_RewardsDistributed = source('mirror_optimism','WritingEditions_evt_RewardsDistributed')
        )
    }}
