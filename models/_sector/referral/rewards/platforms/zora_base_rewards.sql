{{ config(
    schema = 'zora_base',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'view'
    )
}}

WITH rewards_cte as (
    {{zora_referral_rewards(
        blockchain = "base"
        ,ProtocolRewards_evt_RewardsDeposit = source('zora_base','ProtocolRewards_evt_RewardsDeposit')
        )
    }}
)

{{ expand_referral_rewards(
    blockchain='base'
    ,rewards_cte='rewards_cte') }}
