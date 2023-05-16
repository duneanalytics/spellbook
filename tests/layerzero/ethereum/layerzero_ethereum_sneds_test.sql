WITH raw_data AS (
    SELECT s.call_block_time AS raw_block_time,
        call_tx_hash AS raw_tx_hash
    FROM {{ source ('layerzero_ethereum', 'Endpoint_call_send') }}
    WHERE call_success
        AND call_block_time >= '2023-04-01'
        AND call_block_time < '2023-05-01'
)

processed_data AS (
    SELECT block_time,
        tx_hash
    FROM {{ ref('layerzero_ethereum_send') }}
    WHERE block_time >= '2023-04-01'
        AND block_time < '2023-05-01'
)

SELECT *
FROM raw_data r
FULL JOIN processed_data n ON r.raw_block_time = n.block_time AND r.raw_tx_hash = n.tx_hash
WHERE r.raw_tx_hash IS NULL 
    OR n.tx_hash IS NULL
