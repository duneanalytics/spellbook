SELECT DISTINCT
    "from " AS address,
    'eth2 depositor' AS label,
    'hagaetc' AS author,
    'eth2 actions' AS type
FROM
    ethereum. "traces"
WHERE
    block_number >= 11182202
    AND "to" = '\x00000000219ab540356cBB839Cbe05303d7705Fa'
    AND success = TRUE
    AND block_time >= '{{timestamp}}'
