WITH raw_data AS (
    SELECT call_block_time AS raw_block_time,
        call_tx_hash AS raw_tx_hash,
        call_trace_address AS raw_trace_address,
        _dstChainId AS raw_destination_chain_id
    FROM {{ source ('layerzero_bnb', 'Endpoint_call_send') }}
    WHERE (call_block_number = 28339930 AND call_tx_hash = '0xf104f9abce4fa366f1396f0dbb9778ce769756a0cfdc34585181b9d9f2b6e213')
        OR (call_block_number = 28337122 AND call_tx_hash = '0x0e174c31e25d1a7d884c9494f077bdcc982315e0ec696b2a6fc05dff8aa99c50')
        OR (call_block_number = 28337675 AND call_tx_hash = '0x7f74b34068ef67c75882cbd332a7bea7e34e816ef53f9e00b4be7f1cef1bd826')
        OR (call_block_number = 28337609 AND call_tx_hash = '0xef6d892eeb8d60a2c21491a8cbeb5c5fa7ca70a61a44948ef58a2a84882c30d0')
)

processed_data AS (
    SELECT block_time,
        tx_hash,
        trace_address,
        destination_chain_id
    FROM {{ ref('layerzero_bnb_send') }}
    WHERE (block_number = 28339930 AND tx_hash = '0xf104f9abce4fa366f1396f0dbb9778ce769756a0cfdc34585181b9d9f2b6e213')
        OR (block_number = 28337122 AND tx_hash = '0x0e174c31e25d1a7d884c9494f077bdcc982315e0ec696b2a6fc05dff8aa99c50')
        OR (block_number = 28337675 AND tx_hash = '0x7f74b34068ef67c75882cbd332a7bea7e34e816ef53f9e00b4be7f1cef1bd826')
        OR (block_number = 28337609 AND tx_hash = '0xef6d892eeb8d60a2c21491a8cbeb5c5fa7ca70a61a44948ef58a2a84882c30d0')
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
