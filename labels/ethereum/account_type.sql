SELECT DISTINCT
    address,
    'Contract' AS label,
    'Account Type' AS type,
    'sui414' AS author
FROM ethereum."traces"
WHERE type = 'create' and success is true
