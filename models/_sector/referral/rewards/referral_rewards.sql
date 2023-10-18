{{ config(
    schema = 'referral',
    alias = alias('rewards'),
    tags = ['dunesql'],
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum","optimism","arbitrum"]\',
                    "sector",
                    "referral",
                    \'["0xRob"]\') }}')
}}


{{ enrich_referral_rewards (
    model = ref('referral_staging_rewards')
)
}}


