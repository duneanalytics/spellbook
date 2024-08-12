{{ config(
    schema = 'rabbithole_optimism',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{rabbithole_referral_rewards(
        blockchain = "optimism"
        ,QuestFactory_evt_MintFeePaid = source('boost_optimism','QuestFactory_evt_MintFeePaid')
        )
    }}
