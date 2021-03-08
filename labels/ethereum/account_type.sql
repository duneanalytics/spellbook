SELECT DISTINCT
    address,
    'contract' AS label,
    'account type' AS type,
    'sui414' AS author
FROM ethereum."traces"
WHERE type = 'create' and success is true
