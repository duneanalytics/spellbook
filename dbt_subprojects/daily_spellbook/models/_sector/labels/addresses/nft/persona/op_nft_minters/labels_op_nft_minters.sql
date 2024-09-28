{{config(
     alias = 'op_nft_minters'
)}}

WITH erc721_mints AS
(
SELECT "from" AS minter, COUNT("from") AS mint_count
FROM (SELECT nft.evt_tx_hash, tx."from"
FROM {{ source('erc721_optimism', 'evt_Transfer') }} nft
JOIN {{ source('optimism', 'transactions') }} tx
ON nft.evt_tx_hash = tx.hash
WHERE nft."from" = 0x0000000000000000000000000000000000000000
)
GROUP BY "from"
),

erc1155_mints_joined AS
( SELECT minter, SUM(mint_count) AS mint_count
FROM (SELECT operator AS minter, COUNT(operator) AS mint_count
FROM {{ source('erc1155_optimism', 'evt_TransferBatch') }}
WHERE "from" = 0x0000000000000000000000000000000000000000
GROUP BY operator

UNION ALL

SELECT operator AS minter, COUNT(operator) AS mint_count
FROM {{ source('erc1155_optimism', 'evt_TransferSingle') }}
WHERE "from" = 0x0000000000000000000000000000000000000000
GROUP BY operator
)
GROUP BY minter
),

nft_mint_count AS
(SELECT minter, SUM(mint_count) AS mint_count
FROM (SELECT *
FROM erc1155_mints_joined

UNION ALL

SELECT *
FROM erc721_mints
)
GROUP BY minter
),

percentile_nft_mints AS
(SELECT approx_percentile (mint_count, 0.95) AS "0.95p", approx_percentile(mint_count, 0.65) AS "0.65p"
FROM nft_mint_count),

nft_minters AS
(SELECT minter AS address,
(CASE 
WHEN mint_count >= (SELECT "0.95p" FROM percentile_nft_mints) THEN 'Voracious NFT Minter'
WHEN mint_count >= (SELECT "0.65p" FROM percentile_nft_mints) THEN 'Active NFT Minter'
ELSE 'Normie NFT Minter'
END) AS label
FROM nft_mint_count
)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'nft' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-06' AS created_at,
    NOW() AS updated_at,
    'op_nft_minters' AS model_name,
    'persona' AS label_type
FROM
    nft_minters