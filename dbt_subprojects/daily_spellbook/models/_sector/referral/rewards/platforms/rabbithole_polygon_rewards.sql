{{ config(
    schema = 'rabbithole_polygon',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{rabbithole_referral_rewards(
        blockchain = "polygon"
        ,QuestFactory_evt_MintFeePaid = source('boost_polygon','QuestFactory_evt_MintFeePaid')
        ,native_currency_contract = '0x0000000000000000000000000000000000001010'
        )
    }}
