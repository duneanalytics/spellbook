 {{
  config(
        schema = 'solana_utils',
        alias = alias('total_rewards'),
        materialized='table',
        tags = ['dunesql'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

SELECT
recipient
, reward_type
, sum(lamports/pow(10,9)) as rewards
FROM {{ source('solana','rewards') }}
GROUP BY 1,2