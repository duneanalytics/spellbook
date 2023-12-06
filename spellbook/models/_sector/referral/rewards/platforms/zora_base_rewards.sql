{{ config(
    schema = 'zora_base',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{zora_referral_rewards(
    blockchain = "base"
    ,ProtocolRewards_evt_RewardsDeposit = source('zora_base','ProtocolRewards_evt_RewardsDeposit')
    )
}}
