{{ config(
    schema = 'referral',
    alias = 'rewards',
    
    partition_by = ['blockchain','project','block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(\'["ethereum","optimism","arbitrum"]\',
                    "sector",
                    "referral",
                    \'["0xRob"]\') }}')
}}


{{ enrich_referral_rewards (
    model = ref('referral_staging_rewards')
)
}}
