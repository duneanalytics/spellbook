SELECT
    exchange_contract_address AS address,
    CASE 
        WHEN project = 'Gnosis Protocol' THEN 'gnosis'
        WHEN project = 'Bancor Network' THEN 'bancor'
        ELSE lower(project)
    END AS label,
    'owner' AS "type",
    'milkyklim' AS author
FROM
    dex.trades
WHERE
    block_time >= '{{timestamp}}'
;

