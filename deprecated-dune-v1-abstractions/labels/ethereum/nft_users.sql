SELECT
    buyer AS address,
    LOWER(platform) || ' user' AS label,
    'dapp usage' AS type,
    'masquot' AS author
FROM nft.trades
WHERE
    block_time >= '{{timestamp}}'
UNION
SELECT
    seller AS address,
    LOWER(platform) || ' user' AS label,
    'dapp usage' AS type,
    'masquot' AS author
FROM nft.trades
WHERE
    block_time >= '{{timestamp}}';
