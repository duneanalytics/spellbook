SELECT 
    address,
    'contract' AS label,
    'account type' AS type,
    'sui414' AS author
FROM ethereum.traces
WHERE "type" = 'create'
AND success
AND block_time >= '{{timestamp}}';

