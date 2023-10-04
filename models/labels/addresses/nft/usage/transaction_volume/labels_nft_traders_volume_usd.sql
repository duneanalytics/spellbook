{{config(
    tags=['dunesql']
    , alias = alias('nft_traders_volume_usd')
)}}

WITH nft_trades AS (
SELECT
    blockchain,
    amount_usd,
    buyer AS address
FROM {{ ref('nft_trades') }}

UNION

SELECT
    blockchain,
    amount_usd,
    seller AS address
FROM {{ ref('nft_trades') }}
),

total as (
SELECT
address,
SUM(amount_usd) AS total_count
FROM nft_trades
GROUP BY 1
)

SELECT * FROM (
    SELECT
    nft_trades.blockchain as blockchain,
    nft_trades.address,
    CASE WHEN ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) <= 10
              AND ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) > 5
            THEN 'Top 10% NFT Trader (Volume in $USD)'
         WHEN ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) <= 5
              AND ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) > 1
            THEN 'Top 5% NFT Trader (Volume in $USD)'
         WHEN ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) <= 1
            THEN 'Top 1% NFT Trader (Volume in $USD)' END AS name,
    'nft' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-08-24'  as created_at,
    now() as updated_at,
    'nft_traders_volume_usd' as model_name,
    'usage' as label_type
    FROM nft_trades
      JOIN total on total.address = nft_trades.address
    WHERE nft_trades.address is not null and amount_usd is not null
    GROUP BY nft_trades.address, total_count, nft_trades.blockchain
)
WHERE name is not null

