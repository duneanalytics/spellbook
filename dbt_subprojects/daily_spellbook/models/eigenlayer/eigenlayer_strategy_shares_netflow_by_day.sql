{{ 
    config(
        schema = 'eigenlayer',
        alias = 'strategy_shares_netflow_by_day',
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
        date
    FROM {{ ref('eigenlayer_strategy_shares_inflow_by_day') }}

    UNION ALL

    SELECT
        strategy,
        share,
        date
    FROM {{ ref('eigenlayer_strategy_shares_outflow_by_day') }}
)
SELECT
    strategy,
    SUM(share) as share,
    date
FROM union
GROUP BY strategy, date
ORDER BY date DESC
