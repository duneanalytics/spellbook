WITH raw_data AS (
    SELECT call_block_time AS raw_block_time,
        call_tx_hash AS raw_tx_hash,
        call_trace_address AS raw_trace_address,
        _dstChainId AS raw_destination_chain_id
    FROM {{ source ('layerzero_fantom_endpoint_fantom', 'Endpoint_call_send') }}
    WHERE (call_block_number = 62650162 AND call_tx_hash = '0xbcab2a773c110cebc25d6a78cb1a3694113102f05e8c3824f83cc7e92eaf2f87')
        OR (call_block_number = 62640667 AND call_tx_hash = '0x94a3ccc0096566db98e7914dfea4cd8371af5a32ef970c8ccb529fc0f115664b')
        OR (call_block_number = 62651604 AND call_tx_hash = '0xfe2254393b903945a4122d362cc1af7b4572556c0701828830bac739217db170')
        OR (call_block_number = 62637936 AND call_tx_hash = '0xff7e84e27d6c772cfb536f397a8008548e92300737fef052482d1ca6f3313f4b')
),

processed_data AS (
    SELECT block_time,
        tx_hash,
        trace_address,
        destination_chain_id
    FROM {{ ref('layerzero_fantom_send') }}
    WHERE (block_number = 62650162 AND tx_hash = '0xbcab2a773c110cebc25d6a78cb1a3694113102f05e8c3824f83cc7e92eaf2f87')
        OR (block_number = 62640667 AND tx_hash = '0x94a3ccc0096566db98e7914dfea4cd8371af5a32ef970c8ccb529fc0f115664b')
        OR (block_number = 62651604 AND tx_hash = '0xfe2254393b903945a4122d362cc1af7b4572556c0701828830bac739217db170')
        OR (block_number = 62637936 AND tx_hash = '0xff7e84e27d6c772cfb536f397a8008548e92300737fef052482d1ca6f3313f4b')
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
