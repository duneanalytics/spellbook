{{config(
    tags=['dunesql']
    , alias = alias('nft_traders_transactions')
)}}

WITH nft_trades AS (
SELECT
    blockchain,
    tx_hash,
    buyer AS address
FROM {{ ref('nft_trades') }}

UNION

SELECT
    blockchain,
    tx_hash,
    seller AS address
FROM {{ ref('nft_trades') }}
),

total as (
SELECT
address,
COUNT(tx_hash) AS total_count
FROM nft_trades
GROUP BY 1
)

SELECT * FROM (
    SELECT
    nft_trades.blockchain as blockchain,
    nft_trades.address,
    CASE WHEN ((ROW_NUMBER() OVER(ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) <= 10
              AND ((ROW_NUMBER() OVER(ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) > 5
            THEN 'Top 10% NFT Trader (Transactions)'
         WHEN ((ROW_NUMBER() OVER(ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) <= 5
              AND ((ROW_NUMBER() OVER(ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) > 1
            THEN 'Top 5% NFT Trader (Transactions)'
         WHEN ((ROW_NUMBER() OVER(ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) <= 1
            THEN 'Top 1% NFT Trader (Transactions)' END AS name,
    'nft' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-08-24'  as created_at,
    now() as updated_at,
    'nft_traders_transactions' as model_name,
    'usage' as label_type
    FROM nft_trades
      JOIN total on total.address = nft_trades.address
    WHERE nft_trades.address is not null
    GROUP BY nft_trades.address, total_count, nft_trades.blockchain
)
WHERE name is not null

