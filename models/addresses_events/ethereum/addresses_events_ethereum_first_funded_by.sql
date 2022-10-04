{{ config(alias='first_funded_by', materialized = 'table', file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "addresses_events",
                                    \'["hildobby"]\') }}') }}

SELECT b.to AS address
, MIN(a.from) AS first_funded_by
, MIN(a.block_time) AS block_time
, MIN(a.block_number) AS block_number
, MIN(a.tx_hash) AS tx_hash
FROM {{ source('ethereum', 'traces') }} a
JOIN (
    SELECT to
    , MIN(block_number) AS first_block
    FROM {{ source('ethereum', 'traces') }}
    WHERE success
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
    AND value > 0
    GROUP BY to
    ) AS b ON a.to = b.to AND a.block_number = b.first_block
WHERE a.success
AND (a.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR a.call_type IS NULL)
AND a.value > 0
GROUP BY b.to