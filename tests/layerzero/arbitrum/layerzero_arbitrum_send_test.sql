WITH raw_data AS (
    SELECT call_block_time AS raw_block_time,
        call_tx_hash AS raw_tx_hash,
        call_trace_address AS raw_trace_address,
        _dstChainId AS raw_destination_chain_id
    FROM {{ source ('layerzero_arbitrum', 'Endpoint_call_send') }}
    WHERE (call_block_number = 92172312 AND call_tx_hash = '0xaec5fbb5eeab5a7cad3f6e7863beaec0c97923cf96a3925e42a3a3fb6f81485b')
        OR (call_block_number = 92147557 AND call_tx_hash = '0x6256d390513065c9e72e1f9d2e0725338bdd12f8982c87a7577e667dfd2545b6')
        OR (call_block_number = 92167999 AND call_tx_hash = '0xf4473045b74611ef5ac1097eff8f2145a8b738d5e8eea758cef3b483f486fb61')
        OR (call_block_number = 92169669 AND call_tx_hash = '0x732c0557383923a6d48b76a9e6321d09efe82d31e6174816c1284d03ed4c5a17')
)

processed_data AS (
    SELECT block_time,
        tx_hash,
        trace_address,
        destination_chain_id
    FROM {{ ref('layerzero_arbitrum_send') }}
    WHERE (block_number = 92172312 AND tx_hash = '0xaec5fbb5eeab5a7cad3f6e7863beaec0c97923cf96a3925e42a3a3fb6f81485b')
        OR (block_number = 92147557 AND tx_hash = '0x6256d390513065c9e72e1f9d2e0725338bdd12f8982c87a7577e667dfd2545b6')
        OR (block_number = 92167999 AND tx_hash = '0xf4473045b74611ef5ac1097eff8f2145a8b738d5e8eea758cef3b483f486fb61')
        OR (block_number = 92169669 AND tx_hash = '0x732c0557383923a6d48b76a9e6321d09efe82d31e6174816c1284d03ed4c5a17')
)

SELECT *
FROM raw_data r
FULL JOIN processed_data n
    ON r.raw_block_time = n.block_time
    AND r.raw_tx_hash = n.tx_hash
    AND r.raw_trace_address = n.trace_address
    AND r.raw_destination_chain_id = n.destination_chain_id
WHERE r.raw_tx_hash IS NULL 
    OR n.tx_hash IS NULL
