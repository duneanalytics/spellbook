{{config(
    tags=['dunesql']
    , alias = alias('nft_users_platforms')
)}}

WITH nft_trades AS (
SELECT
    blockchain,
    project,
    buyer AS address
FROM {{ ref('nft_trades') }}
        UNION
SELECT
    blockchain,
    project,
    seller AS address
FROM {{ ref('nft_trades') }}
)

SELECT
    blockchain as blockchain,
    address,
    array_join(ARRAY_AGG(DISTINCT concat(upper(substr(project,1,1)),substring(project,2))), ', ') ||' User' as name,
    'nft' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-09-03'  as created_at,
    now() as updated_at,
    'nft_users_platforms' as model_name,
    'persona' as label_type
FROM nft_trades
WHERE address is not null
GROUP BY address, blockchain
