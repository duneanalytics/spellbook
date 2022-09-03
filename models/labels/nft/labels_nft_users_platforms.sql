{{config(alias='nft_users_platforms')}}

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
    array_agg(DISTINCT blockchain) as blockchain,
    address,
    array_join(collect_set(CONCAT(UPPER(SUBSTRING(project,1,1)),LOWER(SUBSTRING(project,2)))), ', ') ||' User' as name,
    'nft' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-10-03') as created_at,
    now() as updated_at
FROM nft_trades
WHERE address is not null
GROUP BY address
