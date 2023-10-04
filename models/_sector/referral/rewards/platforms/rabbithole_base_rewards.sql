{{ config(
    schema = 'rabbithole_base',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'view'
    )
}}

WITH rewards_cte as (
    {{rabbithole_referral_rewards(
        blockchain = "base"
        ,QuestFactory_evt_MintFeePaid = source('rabbithole_base','QuestFactory_evt_MintFeePaid')
        )
    }}
)

{{ expand_referral_rewards(
    blockchain='base'
    ,rewards_cte='rewards_cte') }}
