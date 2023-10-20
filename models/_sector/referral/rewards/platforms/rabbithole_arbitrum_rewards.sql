{{ config(
    schema = 'rabbithole_arbitrum',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{rabbithole_referral_rewards(
        blockchain = "arbitrum"
        ,QuestFactory_evt_MintFeePaid = source('rabbithole_arbitrum','QuestFactory_evt_MintFeePaid')
        )
    }}
