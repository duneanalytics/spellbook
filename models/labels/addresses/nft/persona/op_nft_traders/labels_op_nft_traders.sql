
{{config(
     alias = 'op_nft_traders'
)}}

WITH nft_trades_raw AS
(SELECT buyer AS trader
FROM {{ ref('nft_trades') }}
WHERE blockchain = 'optimism'

UNION ALL

SELECT seller AS trader
FROM {{ ref('nft_trades') }}
WHERE blockchain = 'optimism'
),

nft_trades_count AS
(SELECT trader, COUNT(trader) AS trades_count
FROM nft_trades_raw 
GROUP BY trader
),

percentile_nft_trades AS
(SELECT approx_percentile (trades_count, 0.95) AS "0.95p", 
approx_percentile(trades_count, 0.65) AS "0.65p"
FROM nft_trades_count),

nft_traders AS
(SELECT trader AS address,
(CASE 
WHEN trades_count >= (SELECT "0.95p" FROM percentile_nft_trades) THEN 'Elite NFT Trader'
WHEN trades_count >= (SELECT "0.65p" FROM percentile_nft_trades) THEN 'Active NFT Trader'
ELSE 'Normie NFT Trader'
END) AS label
FROM nft_trades_count
)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'nft' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-06' AS created_at,
    NOW() AS updated_at,
    'op_nft_traders' AS model_name,
    'persona' AS label_type
FROM
    nft_traders
