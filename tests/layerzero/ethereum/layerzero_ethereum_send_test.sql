WITH raw_data AS (
    SELECT call_block_time AS raw_block_time,
        call_tx_hash AS raw_tx_hash,
        call_trace_address AS raw_trace_address,
        _dstChainId AS raw_destination_chain_id
    FROM {{ source ('layerzero_ethereum', 'Endpoint_call_send') }}
    WHERE (call_block_number = 17223223 AND call_tx_hash = '0x6166ba6961e44de378da178061543b37b44a8606061859956f26483d35918259')
        OR (call_block_number = 17224063 AND call_tx_hash = '0xe707457c57f5b135e8550a5033cc7ded4778eed810e10b230b8241414c4666ec')
        OR (call_block_number = 17248461 AND call_tx_hash = '0x1e101a0ec890cd68135b74dd47ddc3795c6198ee5138952c0ba2afbd977c2d2e')
        OR (call_block_number = 17249905 AND call_tx_hash = '0xe26e9fd0a22b2e78c49ad47af15a52c8d85a77015b9e0218ea2d4f049e78d5ef')
)

processed_data AS (
    SELECT block_time,
        tx_hash,
        trace_address,
        destination_chain_id
    FROM {{ ref('layerzero_ethereum_send') }}
    WHERE (block_number = 17223223 AND tx_hash = '0x6166ba6961e44de378da178061543b37b44a8606061859956f26483d35918259')
        OR (block_number = 17224063 AND tx_hash = '0xe707457c57f5b135e8550a5033cc7ded4778eed810e10b230b8241414c4666ec')
        OR (block_number = 17248461 AND tx_hash = '0x1e101a0ec890cd68135b74dd47ddc3795c6198ee5138952c0ba2afbd977c2d2e')
        OR (block_number = 17249905 AND tx_hash = '0xe26e9fd0a22b2e78c49ad47af15a52c8d85a77015b9e0218ea2d4f049e78d5ef')
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
