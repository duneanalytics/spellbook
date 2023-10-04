{{ config(
    schema = 'zora_ethereum',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'view'
    )
}}

WITH rewards_cte as (
    {{zora_referral_rewards(
        blockchain = "ethereum"
        ,ProtocolRewards_evt_RewardsDeposit = source('zora_ethereum','ProtocolRewards_evt_RewardsDeposit')
        )
    }}
)

{{ expand_referral_rewards(
    blockchain='ethereum'
    ,rewards_cte='rewards_cte') }}
