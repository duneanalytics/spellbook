{{ 
    config(
        schema = 'eigenlayer',
        alias = 'strategy_shares_outflow_by_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}'
    )
}}


WITH union AS (
    SELECT
        strategy,
        share,
        date_trunc('day', evt_block_time) AS date
    FROM {{ ref('eigenlayer_withdrawal_completed_v1_enriched') }}

    UNION ALL

    SELECT
        strategy,
        share,
        date_trunc('day', evt_block_time) AS date
    FROM {{ ref('eigenlayer_withdrawal_completed_v2_enriched') }}
),
daily_share AS (
    SELECT
        strategy,
        SUM(share) * -1 as share,
        date
    FROM union
    GROUP BY strategy, date
)
SELECT
    a.date,
    b.strategy,
    COALESCE(b.share, 0) as share
FROM {{ ref('eigenlayer_date_series') }} AS a
LEFT JOIN daily_share AS b
    ON a.date = b.date
ORDER BY a.date DESC
