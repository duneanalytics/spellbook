SELECT
    borrower AS address,
    'borrower' AS label,
    'activity' AS type,
    'hagaetc' AS author
FROM
    lending.borrow
WHERE
    block_time >= '{{timestamp}}'
UNION
SELECT
    borrower AS address,
    'loan collateral supplier' AS label,
    'activity' AS type,
    'hagaetc' AS author
FROM
    lending.collateral_change
WHERE
    block_time >= '{{timestamp}}'
UNION
SELECT
    trader_a AS address,
    'dex trader' AS label,
    'activity' AS type,
    'hagaetc' AS author
FROM
    dex.trades
WHERE
    block_time >= '{{timestamp}}'
UNION
SELECT
    trader_b AS address,
    'dex trader' AS label,
    'activity' AS type,
    'hagaetc' AS author
FROM
    dex.trades
WHERE
    block_time >= '{{timestamp}}'
