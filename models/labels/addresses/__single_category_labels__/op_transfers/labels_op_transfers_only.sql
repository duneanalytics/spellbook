{{config(
     alias = 'op_transfers_only'
)}}

WITH transfers_only
(SELECT DISTINCT("from") AS address, 'Transfers Only' AS label
FROM complete_decoded_log
WHERE "from" NOT IN (
SELECT "from"
FROM complete_decoded_log
WHERE event_name != 'Transfer'
)
AND namespace = 'erc20'
)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'op_transfers' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-07' AS created_at,
    NOW() AS updated_at,
    'op_transfers_only' AS model_name,
    'persona' AS label_type
FROM
    nft_wash_traders