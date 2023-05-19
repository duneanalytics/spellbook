WITH raw_data AS (
    SELECT call_block_time AS raw_block_time,
        call_tx_hash AS raw_tx_hash,
        call_trace_address AS raw_trace_address,
        _dstChainId AS raw_destination_chain_id
    FROM {{ source ('layerzero_optimism', 'Endpoint_call_send') }}
    WHERE (call_block_number = 99685803 AND call_tx_hash = '0x640654315d4ae0143907726280c07bcb16a9798a4f5535c1e9f75352c979e706')
        OR (call_block_number = 99685851 AND call_tx_hash = '0xfe5c76629bdade26bd115090f227b72dbf840d79a939f22c25cba6b8db3bab52')
        OR (call_block_number = 99659636 AND call_tx_hash = '0xdad239e137b0659916bd49a6d15851639aac16ba15978e9c18aa3e7e2749db03')
        OR (call_block_number = 99661187 AND call_tx_hash = '0xf9b497c3db4c4d0c82579e099286a987be8438ee20b6c334b06453d747ba8ff5')
),

processed_data AS (
    SELECT block_time,
        tx_hash,
        trace_address,
        destination_chain_id
    FROM {{ ref('layerzero_optimism_send') }}
    WHERE (block_number = 99685803 AND tx_hash = '0x640654315d4ae0143907726280c07bcb16a9798a4f5535c1e9f75352c979e706')
        OR (block_number = 99685851 AND tx_hash = '0xfe5c76629bdade26bd115090f227b72dbf840d79a939f22c25cba6b8db3bab52')
        OR (block_number = 99659636 AND tx_hash = '0xdad239e137b0659916bd49a6d15851639aac16ba15978e9c18aa3e7e2749db03')
        OR (block_number = 99661187 AND tx_hash = '0xf9b497c3db4c4d0c82579e099286a987be8438ee20b6c334b06453d747ba8ff5')
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
