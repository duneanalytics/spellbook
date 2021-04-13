SELECT
    address,
    lower(name) AS label,
    'contract_name' AS type,
    'dune' AS author
FROM
    ethereum.contracts
WHERE
    address IS NOT NULL
AND
    updated_at >= '{{timestamp}}'
UNION
SELECT
    address,
    lower(namespace) AS label,
    'project' AS type,
    'dune' AS author
FROM
    ethereum.contracts
WHERE
    address IS NOT NULL
AND
    updated_at >= '{{timestamp}}';
