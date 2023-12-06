
{{config(
     alias = 'op_perpetual_traders'
)}}

WITH perps_trades_count AS
(SELECT trader, COUNT(trader) AS trades_count
FROM {{ ref('perpetual_trades') }}
WHERE blockchain = 'optimism'
GROUP BY trader
ORDER BY trades_count DESC),

percentile_perp_trades AS
(SELECT approx_percentile (trades_count, 0.95) AS "0.95p", 
approx_percentile(trades_count, 0.65) AS "0.65p"
FROM perps_trades_count),

perp_traders AS
(SELECT trader AS address,
(CASE 
WHEN trades_count >= (SELECT "0.95p" FROM percentile_perp_trades) THEN 'Elite Perp Trader'
WHEN trades_count >= (SELECT "0.65p" FROM percentile_perp_trades) THEN 'Active Perp Trader'
ELSE 'Normie Perp Trader'
END) AS label
FROM perps_trades_count
)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'op_perpetual' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-06' AS created_at,
    NOW() AS updated_at,
    'op_governance_derived_archetype' AS model_name,
    'persona' AS label_type
FROM
    perp_traders
