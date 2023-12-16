{{config(
     alias = 'op_habitual_wash_traders'
)}}

WITH wash_trades_count AS
(SELECT trader, COUNT(trader) AS trade_count
FROM (SELECT buyer AS trader
FROM {{ ref('nft_optimism_wash_trades') }}
WHERE is_wash_trade = true

UNION ALL

SELECT seller AS trader
FROM {{ ref('nft_optimism_wash_trades') }}
WHERE is_wash_trade = true
)
GROUP BY trader
),

percentile_wash_trades AS
(SELECT approx_percentile (trade_count, 0.95) AS "0.95p", approx_percentile(trade_count, 0.65) AS "0.65p"
FROM wash_trades_count),

nft_wash_traders AS
(SELECT trader AS address,
'Habitual NFT Wash Trader' AS label
FROM wash_trades_count
WHERE trade_count IN (SELECT "0.95p" FROM percentile_wash_trades)
)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'nft' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-07' AS created_at,
    NOW() AS updated_at,
    'op_habitual_wash_traders' AS model_name,
    'persona' AS label_type
FROM
    nft_wash_traders