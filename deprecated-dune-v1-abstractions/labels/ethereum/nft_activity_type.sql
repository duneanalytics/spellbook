SELECT
    buyer AS address,
    'nft trader' AS label,
    'activity' AS type,
    'masquot' AS author
FROM nft.trades
WHERE
    block_time >= '{{timestamp}}'
UNION
SELECT
    seller AS address,
    'nft trader' AS label,
    'activity' AS type,
    'masquot' AS author
FROM nft.trades
WHERE
    block_time >= '{{timestamp}}';
