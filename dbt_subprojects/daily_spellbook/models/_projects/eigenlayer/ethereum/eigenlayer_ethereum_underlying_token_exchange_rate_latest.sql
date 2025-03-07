{{
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'underlying_token_exchange_rate_latest',
        materialized = 'table',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}',
        unique_key = ['strategy']
    )
}}



WITH ranked_logs AS (
    SELECT
        strategy,
        exchange_rate,
        ROW_NUMBER() OVER (PARTITION BY strategy ORDER BY block_number DESC) AS rn
    FROM {{ ref('eigenlayer_ethereum_underlying_token_exchange_rate') }}
)
SELECT
    strategy,
    exchange_rate
FROM ranked_logs
WHERE rn = 1