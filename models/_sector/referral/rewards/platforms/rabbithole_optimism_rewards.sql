{{ config(
    schema = 'rabbithole_optimism',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'view'
    )
}}

WITH rewards_cte as (
    {{rabbithole_referral_rewards(
        blockchain = "optimism"
        ,QuestFactory_evt_MintFeePaid = source('rabbithole_optimism','QuestFactory_evt_MintFeePaid')
        )
    }}
)

{{ expand_referral_rewards(
    blockchain='optimism'
    ,rewards_cte='rewards_cte') }}
