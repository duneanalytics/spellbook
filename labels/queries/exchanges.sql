SELECT
    exchange_contract_address AS address,
    'exchange' AS label,
    'wallet type' AS "type",
    'milkyklim' AS author
FROM
    dex.trades
WHERE
    block_time >= '{{timestamp}}'
;
