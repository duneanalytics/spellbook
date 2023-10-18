{{ config(
    schema = 'referral',
    alias = alias('rewards'),
    tags = ['dunesql'],
    partition_by = ['blockchain','project','block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
    post_hook='{{ expose_spells(\'["ethereum","optimism","arbitrum"]\',
                    "sector",
                    "referral",
                    \'["0xRob"]\') }}')
}}


{{ enrich_referral_rewards (
    model = ref('referral_staging_rewards')
)
}}
