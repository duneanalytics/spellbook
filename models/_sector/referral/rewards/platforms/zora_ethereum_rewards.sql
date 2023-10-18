{{ config(
    schema = 'zora_ethereum',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)']
    )
}}

{{zora_referral_rewards(
    blockchain = "ethereum"
    ,ProtocolRewards_evt_RewardsDeposit = source('zora_ethereum','ProtocolRewards_evt_RewardsDeposit')
    )
}}
