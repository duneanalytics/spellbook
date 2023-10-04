{{ config(
    schema = 'rabbithole_arbitrum',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'view'
    )
}}

WITH rewards_cte as (
    {{rabbithole_referral_rewards(
        blockchain = "arbitrum"
        ,QuestFactory_evt_MintFeePaid = source('rabbithole_arbitrum','QuestFactory_evt_MintFeePaid')
        )
    }}
)

{{ expand_referral_rewards(
    blockchain='arbitrum'
    ,rewards_cte='rewards_cte') }}
