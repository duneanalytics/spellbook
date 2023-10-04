{{ config(
    schema = 'zora_optimism',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'view'
    )
}}

WITH rewards_cte as (
    {{zora_referral_rewards(
        blockchain = "optimism"
        ,ProtocolRewards_evt_RewardsDeposit = source('zora_optimism','ProtocolRewards_evt_RewardsDeposit')
        )
    }}
)

{{ expand_referral_rewards(
    blockchain='optimism'
    ,rewards_cte='rewards_cte') }}
