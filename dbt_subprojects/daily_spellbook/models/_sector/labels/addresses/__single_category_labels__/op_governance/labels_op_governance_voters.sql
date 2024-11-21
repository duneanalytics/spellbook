
{{config(
     alias = 'op_governance_voters'
)}}

WITH votes_cast_raw AS
(SELECT *, ROW_NUMBER() OVER (PARTITION BY tx_hash, index ORDER BY index) AS row_number
FROM
(SELECT *
FROM {{ source('optimism', 'logs_decoded') }}
WHERE contract_address =  0xcdf27f107725988f2261ce2256bdfcde8b382b10
AND event_name = 'VoteCastWithParams'

UNION

SELECT *
FROM {{ source('optimism', 'logs_decoded') }}
WHERE contract_address =  0xcdf27f107725988f2261ce2256bdfcde8b382b10
AND event_name = 'VoteCast'
)
),

votes_cast AS
(SELECT vote.tx_hash, tx."from"
FROM votes_cast_raw vote
JOIN (
SELECT *
FROM {{ source('optimism', 'transactions') }} 
WHERE hash IN (SELECT tx_hash FROM votes_cast_raw)
) tx

ON vote.tx_hash = tx.hash
WHERE vote.row_number = 1
),

votes_count AS
(SELECT "from", COUNT("from") AS votes_count
FROM votes_cast 
GROUP BY "from"
ORDER BY votes_count DESC
),

percentile_values_voters AS
(SELECT approx_percentile(votes_count, 0.95) AS "0.95p", approx_percentile(votes_count, 0.65) AS "0.65p"
FROM votes_count),

optimism_voters AS
(SELECT "from" AS address,
(CASE 
WHEN votes_count >= (SELECT "0.95p" FROM percentile_values_voters) THEN 'Avid Optimism Voter'
WHEN votes_count >= (SELECT "0.65p" FROM percentile_values_voters) THEN 'Active Optimism Voter'
ELSE 'Casual Optimism Voter'
END) AS label
FROM votes_count)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'op_governance' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-05' AS created_at,
    NOW() AS updated_at,
    'op_governance_voters' AS model_name,
    'persona' AS label_type
FROM
    optimism_voters
GROUP BY address, label
