SELECT
    buyer AS address,
    LOWER(nft_project_name) AS label,
    'nft collection buyer' AS type,
    'masquot' AS author
FROM nft.trades
WHERE
    block_time >= '{{timestamp}}' AND nft_project_name IS NOT NULL
UNION
SELECT
    seller AS address,
    LOWER(nft_project_name) AS label,
    'nft collection seller' AS type,
    'masquot' AS author
FROM nft.trades
WHERE
    block_time >= '{{timestamp}}' AND nft_project_name IS NOT NULL;
