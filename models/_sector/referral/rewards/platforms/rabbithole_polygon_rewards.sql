{{ config(
    schema = 'rabbithole_polygon',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'view'
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
