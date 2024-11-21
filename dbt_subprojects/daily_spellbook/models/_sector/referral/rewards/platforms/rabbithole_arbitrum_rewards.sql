{{ config(
    schema = 'rabbithole_arbitrum',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{rabbithole_referral_rewards(
        blockchain = "arbitrum"
        ,QuestFactory_evt_MintFeePaid = source('boost_arbitrum','QuestFactory_evt_MintFeePaid')
        )
    }}
