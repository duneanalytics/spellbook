SELECT
    trader_a AS address,
    lower(project) || ' user' AS label,
    'dapp usage' AS type,
    'hagaetc' AS author
FROM
    dex.trades
WHERE
    block_time >= '{{timestamp}}'
UNION
SELECT
    trader_b AS address,
    lower(project) || ' user' AS label,
    'dapp usage' AS type,
    'hagaetc' AS author
FROM
    dex.trades
WHERE
    block_time >= '{{timestamp}}';
