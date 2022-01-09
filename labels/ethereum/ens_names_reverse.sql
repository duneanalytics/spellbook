SELECT
    DISTINCT ON (name) lower(name) AS label,
    (SELECT "from" FROM ethereum."transactions" WHERE block_time >= '{{timestamp}}' AND hash = call_tx_hash LIMIT 1) AS address,
    'ens name reverse' AS type,
    'twodam' AS author
FROM ethereumnameservice."ReverseRegistrar_v2_call_setName"
WHERE call_success
AND call_block_time >= '{{timestamp}}'
-- only select newest record
ORDER BY name, call_block_time DESC
