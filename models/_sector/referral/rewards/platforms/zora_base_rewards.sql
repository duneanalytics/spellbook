{{ config(
    schema = 'zora_base',
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
    {{zora_referral_rewards(
        blockchain = "base"
        ,ProtocolRewards_evt_RewardsDeposit = source('zora_base','ProtocolRewards_evt_RewardsDeposit')
        )
    }}
)

{{ expand_referral_rewards(
    blockchain='base'
    ,rewards_cte='rewards_cte') }}
