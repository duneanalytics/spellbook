SELECT DISTINCT
    "from" AS address,
    'eth2 depositor' AS label,
    'eth2 actions' AS type,
    'hagaetc' AS author
FROM
    ethereum. "traces"
WHERE block_number >= 11182202
AND "to" = '\x00000000219ab540356cBB839Cbe05303d7705Fa'
AND success
AND block_time >= '{{timestamp}}'
