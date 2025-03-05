{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'programmatic_incentive_by_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}',
        unique_key = ['token', 'date']
    )
}}


WITH rewards AS (
    SELECT
        token,
        amount,
        date_trunc('day', evt_block_time) AS date
    FROM
        {{ ref('eigenlayer_ethereum_programmatic_incentive_flattened') }}
)
SELECT
    token,
    SUM(amount) as daily_amount,
    date
FROM
    rewards
GROUP BY token, date
ORDER BY date DESC