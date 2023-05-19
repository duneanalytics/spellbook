WITH raw_data AS (
    SELECT call_block_time AS raw_block_time,
        call_tx_hash AS raw_tx_hash,
        call_trace_address AS raw_trace_address,
        _dstChainId AS raw_destination_chain_id
    FROM {{ source ('layerzero_polygon', 'Endpoint_call_send') }}
    WHERE (call_block_number = 42886684 AND call_tx_hash = '0xcff85c8964004fbba0591ed80b00e4560ceb488c507e2d699e08208a992f466e')
        OR (call_block_number = 42874465 AND call_tx_hash = '0x982c630cb73345bec2dcaed21182dd9720f2bb1a93354fcb74e1cb0a70f13d51')
        OR (call_block_number = 42878320 AND call_tx_hash = '0xbb35c9082a9e884dbd391ec31ce20986ed541dd3ac41331d7a2c45df3ddd088a')
        OR (call_block_number = 42878818 AND call_tx_hash = '0xc3298e52dd264736533573fdf92b3d9f62b1d6bc2467ffd17844c592d660db7a')
),

processed_data AS (
    SELECT block_time,
        tx_hash,
        trace_address,
        destination_chain_id
    FROM {{ ref('layerzero_polygon_send') }}
    WHERE (block_number = 42886684 AND tx_hash = '0xcff85c8964004fbba0591ed80b00e4560ceb488c507e2d699e08208a992f466e')
        OR (block_number = 42874465 AND tx_hash = '0x982c630cb73345bec2dcaed21182dd9720f2bb1a93354fcb74e1cb0a70f13d51')
        OR (block_number = 42878320 AND tx_hash = '0xbb35c9082a9e884dbd391ec31ce20986ed541dd3ac41331d7a2c45df3ddd088a')
        OR (block_number = 42878818 AND tx_hash = '0xc3298e52dd264736533573fdf92b3d9f62b1d6bc2467ffd17844c592d660db7a')
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
