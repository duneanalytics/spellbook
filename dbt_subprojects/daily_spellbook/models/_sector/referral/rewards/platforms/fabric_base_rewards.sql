{{ config(
    schema = 'fabric_base',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{fabric_referral_rewards(
        blockchain = "base"
        ,SubscriptionTokenV1_evt_ReferralPayout = source('fabric_base','SubscriptionTokenV1_evt_ReferralPayout')
        ,SubscriptionTokenV1Factory_call_deploySubscription = source('fabric_base','SubscriptionTokenV1Factory_call_deploySubscription')
        )
    }}
