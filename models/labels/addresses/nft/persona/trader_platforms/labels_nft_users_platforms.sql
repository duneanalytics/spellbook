{{config(
	tags=['legacy'],
	alias = alias('nft_users_platforms', legacy_model=True))}}

WITH nft_trades AS (
SELECT
    blockchain,
    project,
    buyer AS address
FROM {{ ref('nft_trades_legacy') }}
        UNION
SELECT
    blockchain,
    project,
    seller AS address
FROM {{ ref('nft_trades_legacy') }}
)

SELECT
    blockchain as blockchain,
    address,
    array_join(collect_set(concat(upper(substring(project,1,1)),substring(project,2))), ', ') ||' User' as name,
    'nft' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-09-03') as created_at,
    now() as updated_at,
    'nft_users_platforms' as model_name,
    'persona' as label_type
FROM nft_trades
WHERE address is not null
GROUP BY address, blockchain
