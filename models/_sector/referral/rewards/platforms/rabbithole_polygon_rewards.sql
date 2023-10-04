{{ config(
    schema = 'rabbithole_polygon',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)']
    )
}}

WITH rewards_cte as (
    {{rabbithole_referral_rewards(
        blockchain = "polygon"
        ,QuestFactory_evt_MintFeePaid = source('rabbithole_polygon','QuestFactory_evt_MintFeePaid')
        )
    }}
)

{{ expand_referral_rewards(
    blockchain='polygon'
    ,rewards_cte='rewards_cte') }}
