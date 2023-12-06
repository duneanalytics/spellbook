{{config(
     alias = 'op_dex_traders'
)}}

WITH dex_trades_count AS
(SELECT taker, COUNT(taker) AS trades_count
FROM {{ ref('dex_trades') }}
WHERE blockchain = 'optimism'
GROUP BY taker
),

percentile_dex_trades AS
(SELECT approx_percentile (trades_count, 0.95) AS "0.95p", approx_percentile(trades_count, 0.65) AS "0.65p"
FROM dex_trades_count),

dex_traders AS
(SELECT taker AS address,
(CASE 
WHEN trades_count >= (SELECT "0.95p" FROM percentile_dex_trades) THEN 'Elite DEX Trader'
WHEN trades_count >= (SELECT "0.65p" FROM percentile_dex_trades) THEN 'Active DEX Trader'
ELSE 'Normie DEX Trader'
END) AS label
FROM dex_trades_count
)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'dex' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-06' AS created_at,
    NOW() AS updated_at,
    'op_dex_traders' AS model_name,
    'persona' AS label_type
FROM
    dex_traders