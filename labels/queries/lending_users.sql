SELECT
    borrower AS address,
    lower(project) || ' user' AS label,
    'hagaetc' AS author,
    'dapp usage' AS type
FROM
    lending.borrow
WHERE
    block_time >= '{{timestamp}}'
UNION
SELECT
    borrower AS address,
    lower(project) || ' user' AS label,
    'hagaetc' AS author,
    'dapp usage' AS type
FROM
    lending.collateral_change
WHERE
    block_time >= '{{timestamp}}';

